package nl.pancompany.eventstore;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;
import static nl.pancompany.eventstore.State.uninitializedState;
import static nl.pancompany.eventstore.Type.getTypeForAnnotatedParameter;

class InitialStateCreator<T> {

    private final Class<T> stateClass;

    InitialStateCreator(Class<T> stateClass) {
        this.stateClass = stateClass;
    }

    State<T> createState(List<SequencedEvent> events) {
        Map<Type, Constructor<T>> stateConstructors = getStateConstructor();
        if (stateConstructors.isEmpty()) {
            return createEmptyState(events);
        }
        return createState(stateConstructors, events);
    }

    @SuppressWarnings("unchecked")
    private Map<Type, Constructor<T>> getStateConstructor() {
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
        stateClassConstructors.forEach(constructor -> constructor.setAccessible(true));
        return stateClassConstructors.stream().collect(Collectors.toMap(
                this::getEventType,
                identity()
        ));
    }

    private Type getEventType(Constructor<?> stateConstructor) {
        if (stateConstructor.getParameters().length != 1) {
            throw new IllegalArgumentException("State constructor must have exactly one parameter.");
        }
        Class<?> declaredParemeterType = stateConstructor.getParameters()[0].getType();
        Annotation annotation = stateConstructor.getAnnotation(StateCreator.class);
        return getTypeForAnnotatedParameter(annotation, declaredParemeterType);
    }

    @SuppressWarnings("unchecked")
    private State<T> createEmptyState(List<SequencedEvent> events) {
        Constructor<T> noArgConstructor = (Constructor<T>) Arrays.stream(stateClass.getDeclaredConstructors())
                .filter(constructor -> constructor.getParameterCount() == 0)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format("State class must have exactly one no-args " +
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

    private State<T> createState(Map<Type, Constructor<T>> constructors, List<SequencedEvent> events) {
        if (events.isEmpty()) {
            return uninitializedState(stateClass);
        }
        SequencedEvent firstEvent = events.getFirst();
        List<SequencedEvent> unprocessedEvents = events.subList(1, events.size());
        Constructor<T> constructor = constructors.get(firstEvent.type());
        return new State<>(invoke(constructor, firstEvent.payload()), unprocessedEvents);
    }

    private T invoke(Constructor<T> constructor, Object eventPayload) {
        try {
            return constructor.newInstance(eventPayload);
        } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new StateConstructionFailedException(e);
        }
    }

}
