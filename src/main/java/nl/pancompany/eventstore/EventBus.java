package nl.pancompany.eventstore;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class EventBus implements AutoCloseable {

    private final EventStore eventStore;
    private final Map<Type, Set<InvocableMethod>> synchronousEventHandlers = new HashMap<>();
    private final Map<Type, Set<InvocableMethod>> asynchronousEventHandlers = new HashMap<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final Set<InvocableMethod> synchronousResetHandlers = new HashSet<>();
    private final Set<InvocableMethod> asynchronousResetHandlers = new HashSet<>();

    public EventBus(EventStore eventStore) {
        this.eventStore = eventStore;
        Runtime.getRuntime().addShutdownHook(
                Thread.ofPlatform().name("shutdown-hook").unstarted(() -> {
                    try {
                        shutdownExecutor(executor, 5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                })
        );
    }

    @Override
    public void close() {
        try {
            shutdownExecutor(executor, 10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void shutdownExecutor(ExecutorService es, long timeout, TimeUnit unit)
            throws InterruptedException {
        es.shutdown();
        if (!es.awaitTermination(timeout, unit)) {
            es.shutdownNow();
            es.awaitTermination(timeout, unit); // best effort
        }
    }

    public void registerSynchronousEventHandlers(Class<?> eventHandlerClass) {

        Object eventHandlerInstance = getInstance(eventHandlerClass);
        registerResetHandler(eventHandlerClass, eventHandlerInstance, true);
        registerEventHandler(eventHandlerClass, eventHandlerInstance, true);
    }

    public void registerAsynchronousEventHandlers(Class<?> eventHandlerClass) {
        Object eventHandlerInstance = getInstance(eventHandlerClass);
        registerResetHandler(eventHandlerClass, eventHandlerInstance, false);
        registerEventHandler(eventHandlerClass, eventHandlerInstance, false);
    }

    private static Object getInstance(Class<?> eventHandlerClass) {
        try {
            Constructor<?> noArgConstructor = eventHandlerClass.getDeclaredConstructors()[0];
            noArgConstructor.setAccessible(true);
            return noArgConstructor.newInstance();
        } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public void registerSynchronousEventHandlers(Object eventHandlerInstance) {
        registerResetHandler(eventHandlerInstance.getClass(), eventHandlerInstance, true);
        registerEventHandler(eventHandlerInstance.getClass(), eventHandlerInstance, true);
    }

    public void registerAsynchronousEventHandlers(Object eventHandlerInstance) {
        registerResetHandler(eventHandlerInstance.getClass(), eventHandlerInstance, false);
        registerEventHandler(eventHandlerInstance.getClass(), eventHandlerInstance, false);
    }

    private void registerResetHandler(Class<?> eventHandlerClass, Object instance, boolean synchronous) {
        Set<Method> resetHandlerMethods = Arrays.stream(eventHandlerClass.getDeclaredMethods()).filter(
                method -> method.isAnnotationPresent(ResetHandler.class)).collect(Collectors.toSet());
        if (resetHandlerMethods.size() > 1) {
            throw new IllegalArgumentException("Multiple reset handlers are not allowed.");
        }
        resetHandlerMethods.forEach(method -> method.setAccessible(true));
        Set<InvocableMethod> resetHandlers = synchronous ? synchronousResetHandlers : asynchronousResetHandlers;
        resetHandlerMethods.stream().findFirst().ifPresent(method -> resetHandlers.add(new InvocableMethod(instance, method)));
    }

    private void registerEventHandler(Class<?> eventHandlerClass, Object instance, boolean synchronous) {
        Set<Method> eventHandlerMethods = Arrays.stream(eventHandlerClass.getDeclaredMethods()).filter(
                method -> method.isAnnotationPresent(EventHandler.class)).collect(Collectors.toSet());
        eventHandlerMethods.forEach(method -> method.setAccessible(true));
        Map<Type, InvocableMethod> newEventHandlers = eventHandlerMethods.stream().collect(Collectors.toMap(
                this::getEventType,
                method -> new InvocableMethod(instance, method)
        ));
        Map<Type, Set<InvocableMethod>> eventHandlers = synchronous ? synchronousEventHandlers : asynchronousEventHandlers;
        newEventHandlers.keySet().forEach(key -> eventHandlers.computeIfAbsent(key, type -> new HashSet<>())
                .add(newEventHandlers.get(key)));
    }

    private Type getEventType(Method eventHandlerMethod) {
        Class<?> declaredParemeterType = eventHandlerMethod.getParameters()[0].getType();
        EventHandler annotation = eventHandlerMethod.getAnnotation(EventHandler.class);
        if (declaredParemeterType == Object.class && annotation.type().isBlank()) {
            throw new IllegalArgumentException("EventHandler annotation must have a type defined when the first parameter is Object.");
        } else if (declaredParemeterType != Object.class && !annotation.type().isBlank()) {
            throw new IllegalArgumentException("Either declare an @EventHandler(type = ..) with an Object parameter," +
                    "or declare @EventHandler with a typed parameter.");
        } else if (declaredParemeterType == Object.class) {
            return Type.of(annotation.type());
        }
        return Type.of(declaredParemeterType);
    }

    /**
     * Resets and replays events to registered event handlers.
     *
     * @param end end position, exclusive
     */
    public void replay(EventStore.SequencePosition end) {
        List<SequencedEvent> eventsToReplay = eventStore.read(Query.all(), EventStore.ReadOptions.builder()
                .stoppingPosition(end)
                .build());
        synchronousResetHandlers.forEach(EventBus::invoke);
        asynchronousResetHandlers.forEach(resetHandler -> executor.submit(() -> invoke(resetHandler)));
        eventsToReplay.forEach(this::invokeEventHandlers);
    }

    private static void invoke(InvocableMethod instance) {
        try {
            instance.method().invoke(instance.objectWithMethod());
        } catch (IllegalAccessException e) {
            log.warn("Could not invoke reset handler method.", e);
        } catch (InvocationTargetException e) {
            log.warn("Invoked reset handler threw exception", e);
        }
    }

    void invokeEventHandlers(SequencedEvent sequencedEvent) {
        if (asynchronousEventHandlers.containsKey(sequencedEvent.type())) {
            executor.submit(() -> asynchronousEventHandlers.get(sequencedEvent.type())
                    .forEach(instance -> invoke(instance, sequencedEvent)));
        }
        if (synchronousEventHandlers.containsKey(sequencedEvent.type())) {
            synchronousEventHandlers.get(sequencedEvent.type())
                    .forEach(instance -> invoke(instance, sequencedEvent));
        }
    }

    private static void invoke(InvocableMethod instance, SequencedEvent event) {
        if (instance == null) {
            return; // skip event if no handler
        }
        try {
            instance.method().invoke(instance.objectWithMethod(), event.payload());
        } catch (IllegalAccessException e) {
            log.warn("Could not invoke handler method for event {}", event, e);
        } catch (InvocationTargetException e) {
            log.warn("Invoked handler threw exception for event {}", event, e);
        }
    }

}
