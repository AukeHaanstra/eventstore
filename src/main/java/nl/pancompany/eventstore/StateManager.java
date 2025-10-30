package nl.pancompany.eventstore;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static lombok.AccessLevel.PACKAGE;

@Slf4j
@RequiredArgsConstructor(access = PACKAGE)
public class StateManager {

    private final EventStore eventStore;

    @SuppressWarnings("unchecked")
    public <T> State<T> load(T emptyStateInstance, Query query) {
        requireNonNull(emptyStateInstance);
        List<SequencedEvent> events = eventStore.read(query);
        if (events.isEmpty()) {
            return State.emptyState((Class<T>) emptyStateInstance.getClass());
        }
        State<T> state = new State<>(emptyStateInstance, query, events, eventStore);
        return state;
    }

    public <T> State<T> load(Class<T> stateClass, Query query) {
        List<SequencedEvent> events = eventStore.read(query);
        if (events.isEmpty()) {
            return State.emptyState(stateClass);
        }
        Optional<ConstructorCallback<T>> constructorWithEventParamCallBack = getStateConstructorCallback(stateClass);
        boolean createEmptyState = constructorWithEventParamCallBack.isEmpty();

        State<T> state = createEmptyState ? createEmptyState(stateClass, query, events.subList(0 , events.size()), eventStore) :
                // Use the first event for creating the initial state
                createState(constructorWithEventParamCallBack.get(), events.getFirst(), query, events.subList(1, events.size()), eventStore);
        return state;
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

    private Type getEventType(Constructor<?> stateConstructor) {
        Class<?> declaredParemeterType = stateConstructor.getParameters()[0].getType();
        Annotation annotation = stateConstructor.getAnnotation(StateConstructor.class);
        return Type.getType(annotation, declaredParemeterType);
    }

    private <T> State<T> createState(ConstructorCallback<T> constructorCallBack, SequencedEvent first, Query query, List<SequencedEvent> events, EventStore eventStore) {
        if (!constructorCallBack.type().equals(first.type())) {
            throw new StateConstructionFailedException("Initial event type different from event type declared in StateConstructor");
        }
        try {
            return new State<>(constructorCallBack.constructor().newInstance(first.payload()), query, events, eventStore);
        } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new StateConstructionFailedException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> State<T> createEmptyState(Class<T> stateClass, Query query, List<SequencedEvent> events, EventStore eventStore) {
        try {
            Constructor<T> noArgConstructor = (Constructor<T>) stateClass.getDeclaredConstructors()[0];
            noArgConstructor.setAccessible(true);
            return new State<>(noArgConstructor.newInstance(), stateClass, query, events, eventStore);
        } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static class StateConstructionFailedException extends RuntimeException {

        public StateConstructionFailedException(String message) {
            super(message);
        }
        public StateConstructionFailedException(Throwable cause) {
            super(cause);
        }

    }

    private record ConstructorCallback<T>(Type type, Constructor<T> constructor, boolean noArg) {
    }

    public static class StateManagerOptimisticLockingException extends RuntimeException {
        public StateManagerOptimisticLockingException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
