package nl.pancompany.eventstore;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import nl.pancompany.eventstore.record.SequencedEvent;

import java.util.List;
import java.util.Optional;

import static java.util.Collections.emptyList;

@Slf4j
class State<T> {

    private final T entity;
    @Getter
    private final List<SequencedEvent> unprocessedEvents;

    State() {
        this.entity = null;
        this.unprocessedEvents = emptyList();
    }

    State(T entity, List<SequencedEvent> unprocessedEvents) {
        this.entity = entity;
        this.unprocessedEvents = unprocessedEvents;
    }

    boolean isInitialized() {
        return entity != null;
    }

    static <U> State<U> uninitializedState(Class<U> stateClass) {
        return new State<>();
    }

    Optional<T> getState() {
        return Optional.ofNullable(this.entity);
    }

}
