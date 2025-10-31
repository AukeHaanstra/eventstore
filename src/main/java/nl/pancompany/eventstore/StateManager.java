package nl.pancompany.eventstore;

import lombok.extern.slf4j.Slf4j;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static nl.pancompany.eventstore.State.uninitializedState;
import static nl.pancompany.eventstore.Type.getTypeForAnnotatedParameter;

@Slf4j
public class StateManager<T> {

    private final EventStore eventStore;
    private final Class<T> stateClass;
    private final InitialStateCreator<T> initialStateCreator;
    private final Query query;
    private State<T> state;

    private EventStore.SequencePosition sequencePositionLastSourcedEvent;
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
        setEventSourcedCallbacks();
        executeEventSourcedCallbacks();
    }

    private void setEventSourcedCallbacks() {
        Set<Method> stateClassMethods = Arrays.stream(stateClass.getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(EventSourced.class))
                .collect(Collectors.toSet());
        stateClassMethods.forEach(method -> method.setAccessible(true));
        eventSourcedCallbacks = stateClassMethods.stream().collect(Collectors.toMap(
                this::getEventType,
                method -> eventPayload -> invoke(method, eventPayload)
        ));
    }

    private Type getEventType(Method eventSourcedMethod) {
        if (eventSourcedMethod.getParameters().length != 1) {
            throw new IllegalArgumentException("Event handler method must have exactly one parameter.");
        }
        Class<?> declaredParemeterType = eventSourcedMethod.getParameters()[0].getType();
        Annotation annotation = eventSourcedMethod.getAnnotation(EventSourced.class);
        return getTypeForAnnotatedParameter(annotation, declaredParemeterType);
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
        setEventSourcedCallbacks();
        executeEventSourcedCallbacks();
    }

    public void apply(Object eventPayload, Tag tag) {
        apply(eventPayload, Tags.and(tag), Type.of(eventPayload.getClass()));
    }

    public void apply(Object eventPayload, Tags tags) {
        apply(eventPayload, tags, Type.of(eventPayload.getClass()));
    }

    public void apply(Object eventPayload, Tag tag, Type type) {
        apply(eventPayload, Tags.and(tag), type);
    }

    public void apply(Object eventPayload, Tags tags, Type type) {
        if (!state.isInitialized()) {
            state = initialStateCreator.createState(eventPayload); // try create state from event
        } else {
            InvocableEventHandler eventSourcedEventHandler = eventSourcedCallbacks.get(type);
            if (eventSourcedEventHandler != null) {
                eventSourcedEventHandler.invoke(eventPayload);
            } // if there is no eventsourced handler, that is fine, we don't apply the event, but only append it to the event store
        }
        if (sequencePositionLastSourcedEvent == null) { // No events sourced before
            sequencePositionLastSourcedEvent = eventStore.append(new Event(eventPayload, tags.toSet(), type)).get();
        } else { // use query + append condition for storing event
            try {
                sequencePositionLastSourcedEvent = eventStore.append(new Event(eventPayload, tags.toSet(), type), EventStore.AppendCondition.builder()
                        .failIfEventsMatch(query)
                        .after(sequencePositionLastSourcedEvent.value())
                        .build()).get();
            } catch (EventStore.AppendConditionNotSatisfied e) {
                throw new StateManager.StateManagerOptimisticLockingException(
                        "A unmanaged state-modifying event was stored after event sourcing but before applying the " +
                                "current state-modifying (event), please retry.", e);
            }
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

}
