package nl.pancompany.eventstore;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.pancompany.eventstore.EventStore.Event;
import nl.pancompany.eventstore.EventStore.SequencedEvent;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

import static lombok.AccessLevel.PACKAGE;

@Slf4j
@RequiredArgsConstructor(access = PACKAGE)
public class StateManager {

    public class State {

        @Getter
        private final Object stateInstance;
        @Getter
        private final Class<?> stateClass;

        private State(Object stateInstance) {
            this.stateInstance = stateInstance;
            this.stateClass =  stateInstance == null ? null : stateInstance.getClass();
        }

        private State(Object stateInstance, Class<?> stateClass) {
            this.stateInstance = stateInstance;
            this.stateClass = stateClass;
        }

        public void apply(Object event, Tag tag, Type type) {
            if (stateInstance == null) {
                return;
            }
            StateManager.this.apply(event, Tags.and(tag), type);
        }

        public void apply(Object event, Tags tags, Type type) {
            if (stateInstance == null) {
                return;
            }
            StateManager.this.apply(event, tags, type);
        }
    }

    private final EventStore eventStore;
    private Map<Type, InvocableMethod> eventSourcedCallbacks;

    public State load(Object emptyStateInstance, Query query) {
        State state = new State(emptyStateInstance);
        List<SequencedEvent> events = eventStore.read(query);
        if (events.isEmpty()) {
            log.warn("Tried to source non-existent state. Query: {}", query.toString());
            return this.new State(null);
        }
        eventSourcedCallbacks = getEventSourcedCallbacks(state);
        events.forEach(event -> invoke(eventSourcedCallbacks.get(event.type()),  event));
        return state;
    }

    public State load(Class<?> stateClass, Query query) {
        Optional<ConstructorCallback> constructorWithEventParamCallBack = getStateConstructorCallback(stateClass);
        boolean createEmptyState = !constructorWithEventParamCallBack.isPresent();
        List<SequencedEvent> events = eventStore.read(query);
        if (events.isEmpty()) {
            log.warn("Tried to source non-existent state. Query: {}", query.toString());
            return this.new State(null);
        }
        State state = createEmptyState ? createEmptyState(stateClass) :
                // Use the first event for creating the initial state
                createState(constructorWithEventParamCallBack.get(), events.getFirst());
        eventSourcedCallbacks = getEventSourcedCallbacks(state);
        events.subList(createEmptyState ? 0 : 1, events.size()).forEach(event -> invoke(eventSourcedCallbacks.get(event.type()),  event));
        return state;
    }

    private Optional<ConstructorCallback> getStateConstructorCallback(Class<?> stateClass) {
        Set<Constructor<?>> stateClassConstructors = Arrays.stream(stateClass.getDeclaredConstructors()).
                filter(constructor -> constructor.isAnnotationPresent(StateConstructor.class))
                .collect(Collectors.toSet());
        stateClassConstructors.forEach(constructor -> constructor.setAccessible(true));
        return stateClassConstructors.stream()
                .map(constructor ->new ConstructorCallback(getEventType(constructor), constructor, false))
                .findFirst();
    }

    private Map<Type, InvocableMethod> getEventSourcedCallbacks(State state) {
        Set<Method> stateClassMethods = Arrays.stream(state.getStateClass().getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(EventSourced.class))
                .collect(Collectors.toSet());
        stateClassMethods.forEach(method -> method.setAccessible(true));
        return stateClassMethods.stream().collect(Collectors.toMap(
                this::getEventType,
                method -> new InvocableMethod(state.getStateInstance(), method)
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

    public void apply(Object event, Tags tags, Type type) {
        applyChangeInStateModel(eventSourcedCallbacks.get(type), event);
        eventStore.append(new Event(event, tags.toSet(), type));
    }

    private State createState(ConstructorCallback constructorCallBack, SequencedEvent first) {
        if (!constructorCallBack.type().equals(first.type())) {
            throw new StateConstructionFailedException("Initial event type different from event type declared in StateConstructor");
        }
        try {
            return this.new State(constructorCallBack.constructor().newInstance(first.payload()));
        } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new StateConstructionFailedException(e);
        }
    }

    private State createEmptyState(Class<?> stateClass) {
        try {
            Constructor<?> noArgConstructor = stateClass.getDeclaredConstructors()[0];
            noArgConstructor.setAccessible(true);
            return this.new State(noArgConstructor.newInstance());
        } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
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

    private static void applyChangeInStateModel(InvocableMethod instance, Object payload) {
        if (instance == null) {
            return; // skip event if no handler
        }
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

    private record ConstructorCallback(Type type, Constructor<?> constructor, boolean noArg) {
    }
}
