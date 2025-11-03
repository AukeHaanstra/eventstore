package nl.pancompany.eventstore;

import lombok.extern.slf4j.Slf4j;
import nl.pancompany.eventstore.annotation.EventHandler;
import nl.pancompany.eventstore.annotation.ResetHandler;
import nl.pancompany.eventstore.query.Query;
import nl.pancompany.eventstore.query.Type;
import nl.pancompany.eventstore.data.LoggedException;
import nl.pancompany.eventstore.data.ReadOptions;
import nl.pancompany.eventstore.data.SequencePosition;
import nl.pancompany.eventstore.data.SequencedEvent;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@Slf4j
public class EventBus implements AutoCloseable {

    private static final int EXCEPTION_QUEUE_CAPACITY = 100;
    private final EventStore eventStore;
    private final Map<Type, Set<InvocableEventHandler>> synchronousEventHandlers = new HashMap<>();
    private final Map<Type, Set<InvocableEventHandler>> asynchronousEventHandlers = new HashMap<>();
    private final Map<Type, Set<InvocableEventHandler>> synchronousReplayableEventHandlers = new HashMap<>();
    private final Map<Type, Set<InvocableEventHandler>> asynchronousReplayableEventHandlers = new HashMap<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final Set<Runnable> synchronousResetHandlers = new HashSet<>();
    private final Set<Runnable> asynchronousResetHandlers = new HashSet<>();
    private final Queue<LoggedException> loggedExceptions = new ArrayDeque<>(EXCEPTION_QUEUE_CAPACITY);

    EventBus(EventStore eventStore) {
        this.eventStore = eventStore;
        registerShutdownHook();
    }

    public <T> T registerSynchronousEventHandler(Class<T> eventHandlerClass) {
        requireNonNull(eventHandlerClass);
        T eventHandlerInstance = createInstance(eventHandlerClass);
        registerResetHandler(eventHandlerClass, eventHandlerInstance, true);
        registerEventHandler(eventHandlerClass, eventHandlerInstance, true);
        return eventHandlerInstance;
    }

    public <T> T registerAsynchronousEventHandler(Class<T> eventHandlerClass) {
        requireNonNull(eventHandlerClass);
        T eventHandlerInstance = createInstance(eventHandlerClass);
        registerResetHandler(eventHandlerClass, eventHandlerInstance, false);
        registerEventHandler(eventHandlerClass, eventHandlerInstance, false);
        return eventHandlerInstance;
    }

    @SuppressWarnings("unchecked")
    private static <T> T createInstance(Class<T> eventHandlerClass) {
        try {
            Constructor<?> noArgConstructor = Arrays.stream(eventHandlerClass.getDeclaredConstructors())
                    .filter(constructor -> constructor.getParameterCount() == 0)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(String.format("Event handler class must have a no-args " +
                            "constructor. Class: %s", eventHandlerClass.getName())));
            noArgConstructor.setAccessible(true);
            return (T) noArgConstructor.newInstance();
        } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public void registerSynchronousEventHandler(Object eventHandlerInstance) {
        requireNonNull(eventHandlerInstance);
        registerResetHandler(eventHandlerInstance.getClass(), eventHandlerInstance, true);
        registerEventHandler(eventHandlerInstance.getClass(), eventHandlerInstance, true);
    }

    public void registerAsynchronousEventHandler(Object eventHandlerInstance) {
        requireNonNull(eventHandlerInstance);
        registerResetHandler(eventHandlerInstance.getClass(), eventHandlerInstance, false);
        registerEventHandler(eventHandlerInstance.getClass(), eventHandlerInstance, false);
    }

    private void registerResetHandler(Class<?> eventHandlerClass, Object instance, boolean synchronous) {
        Set<Method> resetHandlerMethods = getResetHandlers(eventHandlerClass, instance);
        resetHandlerMethods.forEach(method -> method.setAccessible(true));
        Set<Runnable> resetHandlers = synchronous ? synchronousResetHandlers : asynchronousResetHandlers;
        resetHandlerMethods.forEach(method -> resetHandlers.add(() -> invokeResetHandler(method, instance)));
    }

    /**
     * Recursively find all reset handlers in the inheritance hierarchy, overwriting reset handlers from superclasses
     * with reset handlers from subclasses for the same event type.
     */
    private Set<Method> getResetHandlers(Class<?> clazz, Object instance) {
        if (clazz == null) {
            return new HashSet<>(); // base case
        }
        Set<Method> resetHandlerMethods = getResetHandlers(clazz.getSuperclass(), instance);
        Set<Method> newResetHandlerMethods = Arrays.stream(clazz.getDeclaredMethods()).filter(
                method -> method.isAnnotationPresent(ResetHandler.class)).collect(Collectors.toSet());
        if (newResetHandlerMethods.size() > 1) {
            throw new IllegalArgumentException("Multiple reset handlers per class are not allowed.");
        }
        resetHandlerMethods.addAll(newResetHandlerMethods);
        return resetHandlerMethods;
    }

    private void invokeResetHandler(Method method, Object instance) {
        try {
            method.invoke(instance);
        } catch (IllegalAccessException e) {
            log.warn("Could not invoke reset handler {}", method, e);
            logException(LoggedException.of("Could not invoke reset handler %s".formatted(method), e));
        } catch (InvocationTargetException e) {
            log.warn("Reset handler threw exception. Method: {}", method, e);
            logException(LoggedException.of("Reset handler threw exception. Method: %s".formatted(method), e.getCause()));
        }
    }

    private void registerEventHandler(Class<?> eventHandlerClass, Object instance, boolean synchronous) {
        Map<Type, InvocableEventHandler> newEventHandlers = getNewEventHandlers(eventHandlerClass, instance, false);
        Map<Type, Set<InvocableEventHandler>> eventHandlers = synchronous ? synchronousEventHandlers : asynchronousEventHandlers;
        newEventHandlers.keySet().forEach(key -> eventHandlers.computeIfAbsent(key, type -> new HashSet<>())
                .add(newEventHandlers.get(key)));

        Map<Type, InvocableEventHandler> newReplayableEventHandlers = getNewEventHandlers(eventHandlerClass, instance, true);
        Map<Type, Set<InvocableEventHandler>> replayableEventHandlers = synchronous ? synchronousReplayableEventHandlers : asynchronousReplayableEventHandlers;
        newReplayableEventHandlers.keySet().forEach(key -> replayableEventHandlers.computeIfAbsent(key, type -> new HashSet<>())
                .add(newReplayableEventHandlers.get(key)));
    }

    /**
     * Recursively find all event handlers in the inheritance hierarchy, overwriting eventhandlers from superclasses
     * with eventhandlers from subclasses for the same event type.
     */
    private Map<Type, InvocableEventHandler> getNewEventHandlers(Class<?> clazz, Object instance, boolean onlyReplayable) {
        if (clazz == null) {
            return new HashMap<>(); // base case
        }
        Map<Type, InvocableEventHandler> newEventHandlers = getNewEventHandlers(clazz.getSuperclass(), instance, onlyReplayable);
        Set<Method> eventHandlerMethods = Arrays.stream(clazz.getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(EventHandler.class))
                .filter(method -> !onlyReplayable || isReplayEnabled(method))
                .collect(Collectors.toSet());
        eventHandlerMethods.forEach(method -> method.setAccessible(true));
        newEventHandlers.putAll(eventHandlerMethods.stream().collect(Collectors.toMap(
                this::getEventType,
                method -> event -> invoke(method, instance, event)
        )));
        return newEventHandlers;
    }

    private Type getEventType(Method eventHandlerMethod) {
        if (eventHandlerMethod.getParameters().length != 1) {
            throw new IllegalArgumentException("Event handler method must have exactly one parameter.");
        }
        Class<?> declaredParameterType = eventHandlerMethod.getParameters()[0].getType();
        EventHandler annotation = eventHandlerMethod.getAnnotation(EventHandler.class);
        return Type.getTypeForAnnotatedParameter(annotation, declaredParameterType);
    }

    private boolean isReplayEnabled(Method eventHandlerMethod) {
        EventHandler annotation = eventHandlerMethod.getAnnotation(EventHandler.class);
        return annotation.enableReplay();
    }

    /**
     * Resets and replays all events to registered replayable event handlers, see {@link EventHandler#enableReplay()}.
     */
    synchronized public void replay() {
        replay(null);
    }

    /**
     * Resets and replays events to registered replayable event handlers, see {@link EventHandler#enableReplay()}.
     *
     * @param end end position, exclusive
     */
    synchronized public void replay(SequencePosition end) {
        List<SequencedEvent> eventsToReplay = eventStore.read(Query.all(), ReadOptions.builder()
                .withStoppingPosition(end)
                .build());
        synchronousResetHandlers.forEach(Runnable::run);
        asynchronousResetHandlers.forEach(executor::submit);
        eventsToReplay.forEach(this::invokeReplayableEventHandlers);
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

    void invokeReplayableEventHandlers(SequencedEvent sequencedEvent) {
        if (asynchronousReplayableEventHandlers.containsKey(sequencedEvent.type())) {
            executor.submit(() -> asynchronousReplayableEventHandlers.get(sequencedEvent.type()).forEach(
                    eventHandler -> eventHandler.invoke(sequencedEvent.payload())
            ));
        }
        if (synchronousReplayableEventHandlers.containsKey(sequencedEvent.type())) {
            synchronousReplayableEventHandlers.get(sequencedEvent.type())
                    .forEach(eventHandler -> eventHandler.invoke(sequencedEvent.payload()));
        }
    }

    private void invoke(Method method, Object instance, Object eventPayload) {
        try {
            method.invoke(instance, eventPayload);
        } catch (IllegalAccessException e) {
            log.warn("Could not invoke handler method for event {}", eventPayload, e);
            logException(LoggedException.of("Could not invoke handler method for event %s".formatted(eventPayload), e));
        } catch (InvocationTargetException e) {
            log.warn("Invoked handler threw exception for event {}", eventPayload, e);
            logException(LoggedException.of("Invoked handler threw exception for event %s".formatted(eventPayload), e.getCause()));
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

    private void logException(LoggedException loggedException) {
        synchronized (loggedExceptions) {
            while (loggedExceptions.size() >= EXCEPTION_QUEUE_CAPACITY) {
                loggedExceptions.poll();
            }
            loggedExceptions.offer(loggedException);
        }
    }

    public List<LoggedException> getLoggedExceptions() {
        synchronized (loggedExceptions) {
            return new ArrayList<>(loggedExceptions);
        }
    }

    public boolean hasLoggedExceptions() {
        return !loggedExceptions.isEmpty();
    }

    public void clearLoggedExceptions() {
        synchronized (loggedExceptions) {
            loggedExceptions.clear();
        }
    }

}
