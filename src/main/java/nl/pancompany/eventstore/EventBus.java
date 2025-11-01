package nl.pancompany.eventstore;

import lombok.extern.slf4j.Slf4j;
import nl.pancompany.eventstore.annotation.EventHandler;
import nl.pancompany.eventstore.annotation.ResetHandler;
import nl.pancompany.eventstore.query.Query;
import nl.pancompany.eventstore.query.Type;
import nl.pancompany.eventstore.record.ReadOptions;
import nl.pancompany.eventstore.record.SequencedEvent;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@Slf4j
public class EventBus implements AutoCloseable {

    private final EventStore eventStore;
    private final Map<Type, Set<InvocableEventHandler>> synchronousEventHandlers = new HashMap<>();
    private final Map<Type, Set<InvocableEventHandler>> asynchronousEventHandlers = new HashMap<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final Set<Runnable> synchronousResetHandlers = new HashSet<>();
    private final Set<Runnable> asynchronousResetHandlers = new HashSet<>();

    public EventBus(EventStore eventStore) {
        this.eventStore = eventStore;
        registerShutdownHook();
    }

    public void registerSynchronousEventHandlers(Class<?> eventHandlerClass) {
        requireNonNull(eventHandlerClass);
        Object eventHandlerInstance = createInstance(eventHandlerClass);
        registerResetHandler(eventHandlerClass, eventHandlerInstance, true);
        registerEventHandler(eventHandlerClass, eventHandlerInstance, true);
    }

    public void registerAsynchronousEventHandlers(Class<?> eventHandlerClass) {
        requireNonNull(eventHandlerClass);
        Object eventHandlerInstance = createInstance(eventHandlerClass);
        registerResetHandler(eventHandlerClass, eventHandlerInstance, false);
        registerEventHandler(eventHandlerClass, eventHandlerInstance, false);
    }

    private static Object createInstance(Class<?> eventHandlerClass) {
        try {
            Constructor<?> noArgConstructor = Arrays.stream(eventHandlerClass.getDeclaredConstructors())
                    .filter(constructor -> constructor.getParameterCount() == 0)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(String.format("Event handler class must have a no-args " +
                            "constructor. Class: %s", eventHandlerClass.getName())));
            noArgConstructor.setAccessible(true);
            return noArgConstructor.newInstance();
        } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public void registerSynchronousEventHandlers(Object eventHandlerInstance) {
        requireNonNull(eventHandlerInstance);
        registerResetHandler(eventHandlerInstance.getClass(), eventHandlerInstance, true);
        registerEventHandler(eventHandlerInstance.getClass(), eventHandlerInstance, true);
    }

    public void registerAsynchronousEventHandlers(Object eventHandlerInstance) {
        requireNonNull(eventHandlerInstance);
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
        Set<Runnable> resetHandlers = synchronous ? synchronousResetHandlers : asynchronousResetHandlers;
        resetHandlerMethods.stream().findFirst().ifPresent(method -> resetHandlers.add(() -> invokeResetHandler(method, instance)));
    }

    private static void invokeResetHandler(Method method, Object instance) {
        try {
            method.invoke(instance);
        } catch (IllegalAccessException e) {
            log.warn("Could not invoke reset handler {}", method, e);
        } catch (InvocationTargetException e) {
            log.warn("Reset handler threw exception. Method: {}", method, e);
        }
    }

    private void registerEventHandler(Class<?> eventHandlerClass, Object instance, boolean synchronous) {
        Set<Method> eventHandlerMethods = Arrays.stream(eventHandlerClass.getDeclaredMethods()).filter(
                method -> method.isAnnotationPresent(EventHandler.class)).collect(Collectors.toSet());
        eventHandlerMethods.forEach(method -> method.setAccessible(true));
        Map<Type, InvocableEventHandler> newEventHandlers = eventHandlerMethods.stream().collect(Collectors.toMap(
                this::getEventType,
                method -> event -> invoke(method, instance, event)
        ));
        Map<Type, Set<InvocableEventHandler>> eventHandlers = synchronous ? synchronousEventHandlers : asynchronousEventHandlers;
        newEventHandlers.keySet().forEach(key -> eventHandlers.computeIfAbsent(key, type -> new HashSet<>())
                .add(newEventHandlers.get(key)));
    }

    private Type getEventType(Method eventHandlerMethod) {
        if (eventHandlerMethod.getParameters().length != 1) {
            throw new IllegalArgumentException("Event handler method must have exactly one parameter.");
        }
        Class<?> declaredParameterType = eventHandlerMethod.getParameters()[0].getType();
        EventHandler annotation = eventHandlerMethod.getAnnotation(EventHandler.class);
        return Type.getTypeForAnnotatedParameter(annotation, declaredParameterType);
    }

    /**
     * Resets and replays events to registered event handlers.
     *
     * @param end end position, exclusive
     */
    synchronized public void replay(EventStore.SequencePosition end) {
        List<SequencedEvent> eventsToReplay = eventStore.read(Query.all(), ReadOptions.builder()
                .withStoppingPosition(end)
                .build());
        synchronousResetHandlers.forEach(Runnable::run);
        asynchronousResetHandlers.forEach(executor::submit);
        eventsToReplay.forEach(this::invokeEventHandlers);
    }

    void invokeEventHandlers(SequencedEvent sequencedEvent) {
        if (asynchronousEventHandlers.containsKey(sequencedEvent.type())) {
            executor.submit(() -> asynchronousEventHandlers.get(sequencedEvent.type()).forEach(
                    eventHandler -> eventHandler.invoke(sequencedEvent.payload())
            ));
        }
        if (synchronousEventHandlers.containsKey(sequencedEvent.type())) {
            synchronousEventHandlers.get(sequencedEvent.type())
                    .forEach(eventHandler -> eventHandler.invoke(sequencedEvent.payload()));
        }
    }

    private static void invoke(Method method, Object instance, Object eventPayload) {
        try {
            method.invoke(instance, eventPayload);
        } catch (IllegalAccessException e) {
            log.warn("Could not invoke handler method for event {}", eventPayload, e);
        } catch (InvocationTargetException e) {
            log.warn("Invoked handler threw exception for event {}", eventPayload, e);
        }
    }

    @Override
    public void close() {
        try {
            shutdownExecutor(executor, 10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void shutdownExecutor(ExecutorService executorService, long timeout, TimeUnit unit)
            throws InterruptedException {
        executorService.shutdown();
        if (!executorService.awaitTermination(timeout, unit)) {
            executorService.shutdownNow();
            executorService.awaitTermination(timeout, unit);
        }
    }

    private void registerShutdownHook() {
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

}
