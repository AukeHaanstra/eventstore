package nl.pancompany.eventstore;

import lombok.extern.slf4j.Slf4j;
import nl.pancompany.eventstore.annotation.EventSourced;
import nl.pancompany.eventstore.exception.AppendConditionNotSatisfied;
import nl.pancompany.eventstore.query.Query;
import nl.pancompany.eventstore.query.Tag;
import nl.pancompany.eventstore.query.Tags;
import nl.pancompany.eventstore.query.Type;
import nl.pancompany.eventstore.data.AppendCondition;
import nl.pancompany.eventstore.data.Event;
import nl.pancompany.eventstore.data.SequencePosition;
import nl.pancompany.eventstore.data.SequencedEvent;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static nl.pancompany.eventstore.State.uninitializedState;
import static nl.pancompany.eventstore.query.Type.getTypeForAnnotatedParameter;

@Slf4j
public class StateManager<T> {

    private final EventStore eventStore;
    private final Class<T> stateClass;
    private final InitialStateCreator<T> initialStateCreator;
    private final Query query;
    private State<T> state;

    private SequencePosition sequencePositionLastSourcedEvent;
    private Map<Type, InvocableEventHandler> eventSourcedCallbacks;

    StateManager(EventStore eventStore, Class<T> stateClass, Query query) {
        this.eventStore = eventStore;
        this.stateClass = stateClass;
        this.initialStateCreator = new InitialStateCreator<>(stateClass);
        this.query = query;
        this.state = uninitializedState(stateClass);
    }

    @SuppressWarnings("unchecked")
    void load(T emptyStateInstance) {
        requireNonNull(emptyStateInstance);
        state = new State<>(emptyStateInstance, eventStore.read(query));
        eventSourcedCallbacks = getEventSourcedCallbacks(stateClass);
        executeEventSourcedCallbacks();
    }

    /**
     * Recursively find all *eventsourced* event handlers in the inheritance hierarchy, overwriting eventhandlers from superclasses
     * with eventhandlers from subclasses for the same event type.
     */
    private Map<Type, InvocableEventHandler> getEventSourcedCallbacks(Class<? super T> clazz) {
        if (clazz == null) {
            return new HashMap<>(); // base case
        }
        Map<Type, InvocableEventHandler> eventSourcedCallbacks = getEventSourcedCallbacks(clazz.getSuperclass());
        Set<Method> stateClassMethods = Arrays.stream(clazz.getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(EventSourced.class))
                .collect(Collectors.toSet());
        stateClassMethods.forEach(method -> method.setAccessible(true));
        eventSourcedCallbacks.putAll(stateClassMethods.stream().collect(Collectors.toMap(
                this::getEventType,
                method -> eventPayload -> invoke(method, eventPayload)
        )));
        return eventSourcedCallbacks;
    }

    private Type getEventType(Method eventSourcedMethod) {
        if (eventSourcedMethod.getParameters().length != 1) {
            throw new IllegalArgumentException("Event handler method must have exactly one parameter.");
        }
        Class<?> declaredParameterType = eventSourcedMethod.getParameters()[0].getType();
        Annotation annotation = eventSourcedMethod.getAnnotation(EventSourced.class);
        return getTypeForAnnotatedParameter(annotation, declaredParameterType);
    }

    private void executeEventSourcedCallbacks() {
        List<SequencedEvent> events = state.getUnprocessedEvents();
        events.stream()
                .filter(event -> eventSourcedCallbacks.containsKey(event.type()))
                .forEach(event -> eventSourcedCallbacks.get(event.type()).invoke(event.payload()));
        sequencePositionLastSourcedEvent = events.isEmpty() ? null : events.getLast().position();
    }

    private void invoke(Method method, Object eventPayload) {
        try {
            method.invoke(state.getState().get(), eventPayload);
        } catch (IllegalAccessException e) {
            log.warn("Could not invoke handler method for event {}", eventPayload, e);
        } catch (InvocationTargetException e) {
            log.warn("Invoked handler threw exception for event {}", eventPayload, e);
        }
    }

    void load() {
        List<SequencedEvent> events = eventStore.read(query);
        state = initialStateCreator.createState(events);
        eventSourcedCallbacks = getEventSourcedCallbacks(stateClass);
        executeEventSourcedCallbacks();
    }

    /**
     * Since sourcing state is a resource-intensive operation (requires streaming all sourced events to eventsourced handlers),
     * and since writing to the eventstore is costly because it requires obtaining a write lock,
     * always try to apply as many events at once as possible when applying a state change.
     *
     * See {@link #apply(List)}
     */
    public void apply(Object eventPayload, Tag tag) {
        requireNonNull(eventPayload);
        requireNonNull(tag);
        apply(List.of(Event.of(eventPayload, Type.of(eventPayload.getClass()), Set.of(tag))));
    }

    /**
     * Since sourcing state is a resource-intensive operation (requires streaming all sourced events to eventsourced handlers),
     * and since writing to the eventstore is costly because it requires obtaining a write lock,
     * always try to apply as many events at once as possible when applying a state change.
     *
     * See {@link #apply(List)}
     */
    public void apply(Object eventPayload, Tags tags) {
        requireNonNull(eventPayload);
        requireNonNull(tags);
        apply(List.of(Event.of(eventPayload, Type.of(eventPayload.getClass()), tags.toSet())));
    }

    /**
     * Since sourcing state is a resource-intensive operation (requires streaming all sourced events to eventsourced handlers),
     * and since writing to the eventstore is costly because it requires obtaining a write lock,
     * always try to apply as many events at once as possible when applying a state change.
     *
     * See {@link #apply(List)}
     */
    public void apply(Object eventPayload, Tag tag, Type type) {
        requireNonNull(eventPayload);
        requireNonNull(tag);
        requireNonNull(type);
        apply(List.of(Event.of(eventPayload, type, Set.of(tag))));
    }

    /**
     * Since sourcing state is a resource-intensive operation (requires streaming all sourced events to eventsourced handlers),
     * and since writing to the eventstore is costly because it requires obtaining a write lock,
     * always try to apply as many events at once as possible when applying a state change.
     *
     * See {@link #apply(List)}
     */
    public void apply(Object eventPayload, Tags tags, Type type) {
        requireNonNull(eventPayload);
        requireNonNull(tags);
        requireNonNull(type);
        apply(List.of(Event.of(eventPayload, type, tags.toSet())));
    }

    /**
     * Since sourcing state is a resource-intensive operation (requires streaming all sourced events to eventsourced handlers),
     * and since writing to the eventstore is costly because it requires obtaining a write lock,
     * always try to apply as many events at once as possible when applying a state change.
     */
    public void apply(List<Event> events) {
        requireNonNull(events);
        List<Event> eventsToSourceBack = events;
        if (!state.isInitialized()) {
            state = initialStateCreator.createState(events.getFirst().payload()); // try create state from first event
            eventsToSourceBack = events.subList(1, events.size());
        }
        for (Event event : eventsToSourceBack) {
            InvocableEventHandler eventSourcedEventHandler = eventSourcedCallbacks.get(event.type());
            if (eventSourcedEventHandler != null) {
                eventSourcedEventHandler.invoke(event.payload());
            } // if there is no eventsourced handler, that is fine, we don't apply the event, but only append it to the event store
        }
        // use query + append condition for storing events
        try {
            sequencePositionLastSourcedEvent = eventStore.append(events, AppendCondition.builder()
                    .failIfEventsMatch(query)
                    .after(sequencePositionLastSourcedEvent == null ? null : sequencePositionLastSourcedEvent)
                    .build()).get();
        } catch (AppendConditionNotSatisfied e) {
            throw new StateManager.StateManagerOptimisticLockingException(
                    "An (unmanaged) state-modifying event was stored after event sourcing but before applying the " +
                            "current state-modifying event, please retry.", e);
        }
    }

    public Optional<T> getState() {
        return state.getState();
    }

    public static class StateManagerOptimisticLockingException extends RuntimeException {
        public StateManagerOptimisticLockingException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    @FunctionalInterface
    private interface InvocableEventHandler {
        void invoke(Object event);
    }
}
