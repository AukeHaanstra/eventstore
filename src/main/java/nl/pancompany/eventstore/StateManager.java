package nl.pancompany.eventstore;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import nl.pancompany.eventstore.EventStore.AppendCondition;
import nl.pancompany.eventstore.EventStore.SequencePosition;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static lombok.AccessLevel.PACKAGE;

@Slf4j
@RequiredArgsConstructor(access = PACKAGE)
public class StateManager {

    public class State<T> {

        @Getter
        private final T entity;
        @Getter(PACKAGE)
        private final Class<T> stateClass;
        private final Query query;
        @Setter(PACKAGE)
        private SequencePosition sequencePositionLastSourcedEvent;

        @SuppressWarnings("unchecked")
        private State(T entity, Query query) {
            this.entity = entity;
            this.stateClass = (Class<T>) entity.getClass();
            this.query = query;
        }

        private State(T entity, Class<T> stateClass, Query query) {
            this.entity = entity;
            this.stateClass = stateClass;
            this.query = query;
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
                    eventStore.append(new Event(event, tags.toSet(), type), AppendCondition.builder()
                            .failIfEventsMatch(query)
                            .after(sequencePositionLastSourcedEvent.value())
                            .build());
                } catch (EventStore.AppendConditionNotSatisfied e) {
                    throw new StateManagerOptimisticLockingException(
                            "A state-modifying event was stored after event sourcing but before applying the current " +
                                    "state change (event), please retry.", e);
                }
            }
        }

    }

    private final EventStore eventStore;
    private Map<Type, InvocableMethod> eventSourcedCallbacks;

    public <T> State<T> load(T emptyStateInstance, Query query) {
        requireNonNull(emptyStateInstance);
        List<SequencedEvent> events = eventStore.read(query);
        State<T> state = new State<>(emptyStateInstance, query);
        executeEventSourcedCallbacks(state, events);
        return state;
    }

    public <T> State<T> load(Class<T> stateClass, Query query) {
        Optional<ConstructorCallback<T>> constructorWithEventParamCallBack = getStateConstructorCallback(stateClass);
        boolean createEmptyState = constructorWithEventParamCallBack.isEmpty();
        List<SequencedEvent> events = eventStore.read(query);
        State<T> state = createEmptyState ? createEmptyState(stateClass, query) :
                // Use the first event for creating the initial state
                createState(constructorWithEventParamCallBack.get(), events.getFirst(), query);
        executeEventSourcedCallbacks(state, events.subList(createEmptyState ? 0 : 1, events.size()));
        return state;
    }

    private <T> void executeEventSourcedCallbacks(State<T> state, List<SequencedEvent> events) {
        eventSourcedCallbacks = getEventSourcedCallbacks(state);
        events.stream()
                .map(event -> new EventHandlerInvocation(eventSourcedCallbacks.get(event.type()), event))
                .filter(EventHandlerInvocation::isInvokable)
                .forEach(StateManager::invoke);
        SequencePosition lastEventPosition = events.isEmpty() ? null : events.getLast().position();
        state.setSequencePositionLastSourcedEvent(lastEventPosition);
    }

    @SuppressWarnings("unchecked")
    private <T> Optional<ConstructorCallback<T>> getStateConstructorCallback(Class<T> stateClass) {
        Set<Constructor<T>> stateClassConstructors = Arrays.stream(stateClass.getDeclaredConstructors())
                .map(constructor -> ((Constructor<T>) constructor))
                .filter(constructor -> constructor.isAnnotationPresent(StateConstructor.class))
                .collect(Collectors.toSet());
        stateClassConstructors.forEach(constructor -> constructor.setAccessible(true));
        return stateClassConstructors.stream()
                .map(constructor ->new ConstructorCallback<>(getEventType(constructor), constructor, false))
                .findFirst();
    }

    private <T> Map<Type, InvocableMethod> getEventSourcedCallbacks(State<T> state) {
        Set<Method> stateClassMethods = Arrays.stream(state.getStateClass().getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(EventSourced.class))
                .collect(Collectors.toSet());
        stateClassMethods.forEach(method -> method.setAccessible(true));
        return stateClassMethods.stream().collect(Collectors.toMap(
                this::getEventType,
                method -> new InvocableMethod(state.getEntity(), method)
        ));
    }

    private Type getEventType(Constructor<?> stateConstructor) {
        Class<?> declaredParemeterType = stateConstructor.getParameters()[0].getType();
        Annotation annotation = stateConstructor.getAnnotation(StateConstructor.class);
        return getType(annotation, declaredParemeterType);
    }

    private Type getEventType(Method eventHandlerMethod) {
        Class<?> declaredParemeterType = eventHandlerMethod.getParameters()[0].getType();
        Annotation annotation = eventHandlerMethod.getAnnotation(EventSourced.class);
        return getType(annotation, declaredParemeterType);
    }

    private static Type getType(Annotation annotation, Class<?> declaredParemeterType) {
        String parameterName = annotation.getClass().getSimpleName();
        String type = getAnnotationTypeElementValue(annotation);
        if (declaredParemeterType == Object.class && type.isBlank()) {
            throw new IllegalArgumentException("%s annotation must have a type defined when the first parameter is Object."
                    .formatted(parameterName));
        } else if (declaredParemeterType != Object.class && !type.isBlank()) {
            throw new IllegalArgumentException(String.format("Either declare an @%s(type = ..) with an Object " +
                    "parameter, or declare @%s with a typed parameter.", parameterName, parameterName));
        } else if (declaredParemeterType == Object.class) {
            return Type.of(type);
        }
        return Type.of(declaredParemeterType);
    }

    private static String getAnnotationTypeElementValue(Annotation annotation) {
        String type;
        try {
            Method getType = annotation.getClass().getMethod("type");
            getType.setAccessible(true);
            type = (String) getType.invoke(annotation);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
        return type;
    }

    private <T> State<T> createState(ConstructorCallback<T> constructorCallBack, SequencedEvent first, Query query) {
        if (!constructorCallBack.type().equals(first.type())) {
            throw new StateConstructionFailedException("Initial event type different from event type declared in StateConstructor");
        }
        try {
            return this.new State<>(constructorCallBack.constructor().newInstance(first.payload()), query);
        } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new StateConstructionFailedException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> State<T> createEmptyState(Class<T> stateClass, Query query) {
        try {
            Constructor<T> noArgConstructor = (Constructor<T>) stateClass.getDeclaredConstructors()[0];
            noArgConstructor.setAccessible(true);
            return this.new State<>(noArgConstructor.newInstance(), stateClass, query);
        } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
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

    public class StateConstructionFailedException extends RuntimeException {

        public StateConstructionFailedException(String message) {
            super(message);
        }
        public StateConstructionFailedException(Throwable cause) {
            super(cause);
        }

    }

    private record ConstructorCallback<T>(Type type, Constructor<T> constructor, boolean noArg) {
    }

    public class StateManagerOptimisticLockingException extends RuntimeException {
        public StateManagerOptimisticLockingException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
