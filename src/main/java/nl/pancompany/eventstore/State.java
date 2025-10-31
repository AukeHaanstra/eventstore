package nl.pancompany.eventstore;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

import static java.util.Collections.emptyList;

@Slf4j
class State<T> {

    private final T entity;
    private final Class<T> stateClass;
    @Getter
    private final List<SequencedEvent> unprocessedEvents;

    @SuppressWarnings("unchecked")
    State(T entity, List<SequencedEvent> unprocessedEvents) {
        this(entity, (Class<T>) entity.getClass(), unprocessedEvents);
    }

    State(T entity, Class<T> stateClass, List<SequencedEvent> unprocessedEvents) {
        this.entity = entity;
        this.stateClass = stateClass;
        this.unprocessedEvents = unprocessedEvents;
    }

    boolean isInitialized() {
        return entity != null;
    }

    boolean isUninitialized() {
        return entity == null;
    }

    static <U> State<U> uninitializedState(Class<U> stateClass) {
        return new State<>(null, stateClass, emptyList());
    }

    Optional<T> getState() {
        return Optional.ofNullable(this.entity);
    }

    Class<T> getStateClass() {
        return this.stateClass;
    }

}
