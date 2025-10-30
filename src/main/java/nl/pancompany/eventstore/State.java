package nl.pancompany.eventstore;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

import static lombok.AccessLevel.PACKAGE;

@Slf4j
public class State<T> {

    private final EventStore eventStore;
    private final T entity;
    private final Class<T> stateClass;
    private final Query query;
    @Setter(PACKAGE)
    private EventStore.SequencePosition sequencePositionLastSourcedEvent;
    private Map<Type, InvocableMethod> eventSourcedCallbacks;

    @SuppressWarnings("unchecked")
    State(T entity, Query query, List<SequencedEvent> events, EventStore eventStore) {
        this(entity, (Class<T>) entity.getClass(), query, events, eventStore);
    }

    State(T entity, Class<T> stateClass, Query query, List<SequencedEvent> events, EventStore eventStore) {
        this.entity = entity;
        this.stateClass = stateClass;
        this.query = query;
        this.eventStore = eventStore;
        if (entity != null) {
            this.eventSourcedCallbacks = getEventSourcedCallbacks();
            executeEventSourcedCallbacks(events);
        }
    }

    public static <U> State<U> emptyState(Class<U> stateClass) {
        return new State<>(null, stateClass, null, null, null);
    }

    public void apply(Object event, Tag tag) {
        if (entity == null) {
            return;
        }
        apply(event, Tags.and(tag), Type.of(event.getClass()));
    }

    public void apply(Object event, Tags tags) {
        if (entity == null) {
            return;
        }
        apply(event, tags, Type.of(event.getClass()));
    }


    public void apply(Object event, Tag tag, Type type) {
        if (entity == null) {
            return;
        }
        apply(event, Tags.and(tag), type);
    }

    public void apply(Object event, Tags tags, Type type) {
        InvocableMethod eventSourcedMethod = eventSourcedCallbacks.get(type);
        if (eventSourcedMethod != null) {
            applyChangeInStateModel(eventSourcedMethod, event);
        }
        if (sequencePositionLastSourcedEvent == null) { // No events sourced before
            eventStore.append(new Event(event, tags.toSet(), type));
        } else { // use query + append condition for storing event
            try {
                eventStore.append(new Event(event, tags.toSet(), type), EventStore.AppendCondition.builder()
                        .failIfEventsMatch(query)
                        .after(sequencePositionLastSourcedEvent.value())
                        .build());
            } catch (EventStore.AppendConditionNotSatisfied e) {
                throw new StateManager.StateManagerOptimisticLockingException(
                        "A state-modifying event was stored after event sourcing but before applying the current " +
                                "state change (event), please retry.", e);
            }
        }
    }

    public Optional<T> getEntity() {
        return Optional.ofNullable(this.entity);
    }

    Class<T> getStateClass() {
        return this.stateClass;
    }

    private void executeEventSourcedCallbacks(List<SequencedEvent> events) {
        events.stream()
                .map(event -> new EventHandlerInvocation(eventSourcedCallbacks.get(event.type()), event))
                .filter(EventHandlerInvocation::isInvokable)
                .forEach(State::invoke);
        EventStore.SequencePosition lastEventPosition = events.isEmpty() ? null : events.getLast().position();
        setSequencePositionLastSourcedEvent(lastEventPosition);
    }


    private Map<Type, InvocableMethod> getEventSourcedCallbacks() {
        Set<Method> stateClassMethods = Arrays.stream(getStateClass().getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(EventSourced.class))
                .collect(Collectors.toSet());
        stateClassMethods.forEach(method -> method.setAccessible(true));
        return stateClassMethods.stream().collect(Collectors.toMap(
                this::getEventType,
                method -> new InvocableMethod(getEntity().get(), method)
        ));
    }

    private Type getEventType(Method eventHandlerMethod) {
        Class<?> declaredParemeterType = eventHandlerMethod.getParameters()[0].getType();
        Annotation annotation = eventHandlerMethod.getAnnotation(EventSourced.class);
        return Type.getType(annotation, declaredParemeterType);
    }


    private static void invoke(EventHandlerInvocation eventHandlerInvocation) {
        try {
            eventHandlerInvocation.invocableMethod().method().invoke(
                    eventHandlerInvocation.invocableMethod().objectWithMethod(),
                    eventHandlerInvocation.event().payload());
        } catch (IllegalAccessException e) {
            log.warn("Could not invoke handler method for event {}", eventHandlerInvocation.event(), e);
        } catch (InvocationTargetException e) {
            log.warn("Invoked handler threw exception for event {}", eventHandlerInvocation.event(), e);
        }
    }

    private static void applyChangeInStateModel(InvocableMethod instance, Object payload) {
        try {
            instance.method().invoke(instance.objectWithMethod(), payload);
        } catch (IllegalAccessException e) {
            log.warn("Could not apply state change for event {}", payload, e);
        } catch (InvocationTargetException e) {
            log.warn("Invoked handler threw exception while applying state change {}", payload, e);
        }
    }
}
