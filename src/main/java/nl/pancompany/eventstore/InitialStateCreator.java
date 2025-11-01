package nl.pancompany.eventstore;

import nl.pancompany.eventstore.annotation.StateCreator;
import nl.pancompany.eventstore.exception.StateConstructionFailedException;
import nl.pancompany.eventstore.query.Type;
import nl.pancompany.eventstore.record.Event;
import nl.pancompany.eventstore.record.SequencedEvent;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;
import static nl.pancompany.eventstore.State.uninitializedState;
import static nl.pancompany.eventstore.query.Type.getTypeForAnnotatedParameter;

class InitialStateCreator<T> {

    private final Class<T> stateClass;

    InitialStateCreator(Class<T> stateClass) {
        this.stateClass = stateClass;
    }

    State<T> createState(List<SequencedEvent> events) {
        Map<Type, Constructor<T>> stateConstructors = getStateConstructors();
        if (stateConstructors.isEmpty()) {
            return createEmptyState(events);
        }
        return createState(stateConstructors, events);
    }

    @SuppressWarnings("unchecked")
    private Map<Type, Constructor<T>> getStateConstructors() {
        Set<Constructor<T>> stateClassConstructors = Arrays.stream(stateClass.getDeclaredConstructors())
                .map(constructor -> ((Constructor<T>) constructor))
                .filter(constructor -> constructor.isAnnotationPresent(StateCreator.class))
                .collect(Collectors.toSet());
        Set<Constructor<T>> validStateClassConstructors = stateClassConstructors.stream()
                .filter(constructor -> constructor.getParameterCount() == 1)
                .collect(Collectors.toSet());
        if (validStateClassConstructors.size() != stateClassConstructors.size()) {
            throw new IllegalArgumentException("State Constructors with multiple parameters are not allowed.");
        }
        validStateClassConstructors.forEach(constructor -> constructor.setAccessible(true));
        return stateClassConstructors.stream().collect(Collectors.toMap(
                this::getEventType,
                identity()
        ));
    }

    private Type getEventType(Constructor<?> stateConstructor) {
        Class<?> declaredParameterType = stateConstructor.getParameters()[0].getType();
        Annotation annotation = stateConstructor.getAnnotation(StateCreator.class);
        return getTypeForAnnotatedParameter(annotation, declaredParameterType);
    }

    @SuppressWarnings("unchecked")
    private State<T> createEmptyState(List<SequencedEvent> events) {
        Constructor<T> noArgConstructor = (Constructor<T>) Arrays.stream(stateClass.getDeclaredConstructors())
                .filter(constructor -> constructor.getParameterCount() == 0)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format("State class must have a no-args " +
                        "constructor or an @%s annotated constructor.", StateCreator.class.getSimpleName())));
        noArgConstructor.setAccessible(true);
        return new State<>(invoke(noArgConstructor), events);
    }

    private T invoke(Constructor<T> constructor) {
        try {
            return constructor.newInstance();
        } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new StateConstructionFailedException(e);
        }
    }

    private State<T> createState(Map<Type, Constructor<T>> stateConstructors, List<SequencedEvent> events) {
        if (events.isEmpty()) {
            return uninitializedState(stateClass);
        }
        SequencedEvent firstEvent = events.getFirst();
        List<SequencedEvent> unprocessedEvents = events.subList(1, events.size());
        Constructor<T> constructor = stateConstructors.get(firstEvent.type());
        if (constructor == null) {
            throw new StateConstructionFailedException(String.format("No state constructor found for event type %s.", firstEvent.type()));
        }
        return new State<>(invoke(constructor, firstEvent.payload()), unprocessedEvents);
    }

    State<T> createState(Object firstEventPayload) {
        return createState(Event.of(firstEventPayload, Type.of(firstEventPayload.getClass())));
    }

    State<T> createState(Event firstEvent) {
        Map<Type, Constructor<T>> stateConstructors = getStateConstructors();
        Constructor<T> constructor = stateConstructors.get(firstEvent.type());
        if (constructor == null) {
            throw new StateConstructionFailedException(String.format("No state constructor found for event type %s.", firstEvent.type()));
        }
        return new State<>(invoke(constructor, firstEvent.payload()), Collections.emptyList());
    }

    private T invoke(Constructor<T> constructor, Object eventPayload) {
        try {
            return constructor.newInstance(eventPayload);
        } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new StateConstructionFailedException(e);
        }
    }

}
