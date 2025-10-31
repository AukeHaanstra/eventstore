package nl.pancompany.eventstore;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Objects.requireNonNull;

// TODO: Passage-of-time events https://verraes.net/2019/05/patterns-for-decoupling-distsys-passage-of-time-event/
@Slf4j
public class EventStore implements AutoCloseable {

    private final List<SequencedEvent> storedEvents = new ArrayList<>();
    private final Map<Tag, Set<SequencePosition>> tagPositions = new HashMap<>();
    private final Map<Type, Set<SequencePosition>> typePositions = new HashMap<>();
    private final List<SequencePosition> allSequencePositions = new ArrayList<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();
    @Getter
    private final EventBus eventBus = new EventBus(this);

    @SuppressWarnings("unchecked")
    public <T> StateManager<T> loadState(T emptyStateInstance, Query query) {
        requireNonNull(emptyStateInstance);
        var stateManager = new StateManager<>(this, (Class<T>) emptyStateInstance.getClass(), query);
        stateManager.load(emptyStateInstance);
        return stateManager;
    }

    public <T> StateManager<T> loadState(Class<T> stateClass, Query query) {
        var stateManager = new StateManager<>(this, stateClass, query);
        stateManager.load();
        return stateManager;
    }

    /**
     * Contract: event payload must always be immutable to guarantee immutability of events in the event store
     *
     * @param events The event instances that wrap a payload (the raw event).
     */
    public Optional<SequencePosition> append(Event... events) {
        try {
            return append(List.of(events), null);
        } catch (AppendConditionNotSatisfied e) { // no append condition, so irrelevant
            throw new IllegalStateException(e);
        }
    }

    /**
     * Contract: event payload must always be immutable to guarantee immutability of events in the event store
     *
     * @param events The event instances that wrap a payload (the raw event).
     */
    public Optional<SequencePosition> append(List<Event> events) {
        try {
            return append(events, null);
        } catch (AppendConditionNotSatisfied e) { // no append condition, so irrelevant
            throw new IllegalStateException(e);
        }
    }

    /**
     * Contract: event payload must always be immutable to guarantee immutability of events in the event store
     *
     * @param event The event instances that wrap a payload (the raw event).
     */
    public Optional<SequencePosition> append(Event event, AppendCondition appendCondition) throws AppendConditionNotSatisfied {
        return append(List.of(event), appendCondition);
    }

    /**
     * Contract: event payload must always be immutable to guarantee immutability of events in the event store
     *
     * @param events The event instances that wrap a payload (the raw event).
     */
    public Optional<SequencePosition> append(List<Event> events, AppendCondition appendCondition) throws AppendConditionNotSatisfied {
        List<SequencedEvent> addedEvents = new ArrayList<>();
        SequencePosition lastInsertPosition = null;
        try {
            writeLock.lock();
            if (appendCondition != null) {
                checkWhetherEventsFailAppendCondition(appendCondition);
            }
            for (Event event : events) {
                lastInsertPosition = SequencePosition.of(storedEvents.size());
                SequencedEvent storedEvent = new SequencedEvent(event, lastInsertPosition);
                storedEvents.add(storedEvent);
                addedEvents.add(storedEvent);
                allSequencePositions.add(lastInsertPosition);
                for (Tag tag : event.tags()) {
                    tagPositions.computeIfAbsent(tag, k -> new HashSet<>()).add(lastInsertPosition); // add to tag-index
                }
                typePositions.computeIfAbsent(event.type(), k -> new HashSet<>()).add(lastInsertPosition); // add to type-index
            }
        } finally {
            writeLock.unlock();
        }
        addedEvents.forEach(eventBus::invokeEventHandlers);
        return Optional.ofNullable(lastInsertPosition);
    }

    private void checkWhetherEventsFailAppendCondition(AppendCondition appendCondition) throws AppendConditionNotSatisfied {
        List<SequencedEvent> queryResult = queryEvents(
                appendCondition.failIfEventsMatch(),
                appendCondition.after() == null ? null : ReadOptions.builder()
                        .withStartingPosition(appendCondition.after().incrementAndGet().value()).build());
        if (!queryResult.isEmpty()) {
            if (appendCondition.after() == null) {
                throw new AppendConditionNotSatisfied("An event matched the provided failIfEventsMatch query");
            }
            throw new AppendConditionNotSatisfied("An event matched the provided failIfEventsMatch query after sequence number " + appendCondition.after());
        }
    }

    public List<SequencedEvent> read(Query query) {
        return read(query, null);
    }

    public List<SequencedEvent> read(Query query, ReadOptions options) {
        try {
            readLock.lock();
            return queryEvents(query, options);
        } finally {
            readLock.unlock();
        }
    }

    private List<SequencedEvent> queryEvents(Query query, ReadOptions options) {
        Set<SequencePosition> sequencePositionsFromSelection = getSelectedSequencePositions(options);
        Set<SequencePosition> querySequencePositions = new HashSet<>();
        for (QueryItem queryItem : query.getQueryItems()) {
            if (queryItem.isAll()) {
                return sequencePositionsToEvents(sequencePositionsFromSelection); // just map base set to events
            }
            Set<SequencePosition> queryItemSequencePositions = new HashSet<>(sequencePositionsFromSelection); // mutable base-set of positions
            if (!queryItem.isAllTags()) { // if all, then retain base-set, otherwise:
                for (Tag tag : queryItem.tags()) { // step-wise intersection with the set of positions for each query tag (AND)
                    queryItemSequencePositions.retainAll(tagPositions.computeIfAbsent(tag, t -> new HashSet<>()));
                }
            }
            if (!queryItem.isAllTypes()) { // if all, no second intersection, otherwise:
                Set<SequencePosition> queryItemTypePositions = new HashSet<>();
                for (Type type : queryItem.types()) { // step-wise union of the position sets of all query event types (OR)
                    queryItemTypePositions.addAll(typePositions.computeIfAbsent(type, t -> new HashSet<>()));
                }
                queryItemSequencePositions.retainAll(queryItemTypePositions); // intersection with the set of positions of all query event types (AND)
            }
            querySequencePositions.addAll(queryItemSequencePositions); // union of all sets of positions for all query items (OR)
        }
        return sequencePositionsToEvents(querySequencePositions);
    }

    private Set<SequencePosition> getSelectedSequencePositions(ReadOptions options) {
        Set<SequencePosition> sequencePositionsFromSelection;
        if (options == null) {
            sequencePositionsFromSelection = new HashSet<>(allSequencePositions);
        } else {
            sequencePositionsFromSelection = new HashSet<>(allSequencePositions.subList(
                    options.startingPosition.value,
                    options.stopPosition == null ? allSequencePositions.size() : options.stopPosition.value));
        }
        return sequencePositionsFromSelection;
    }

    private List<SequencedEvent> sequencePositionsToEvents(Set<SequencePosition> querySequencePositions) {
        return querySequencePositions.stream()
                .sorted()
                .map(position -> storedEvents.get(position.value))
                .toList();
    }

    @Override
    public void close() {
        eventBus.close();
    }

    public record SequencePosition(int value) implements Comparable<SequencePosition> {

        public static SequencePosition of(int i) {
            return new SequencePosition(i);
        }

        private SequencePosition incrementAndGet() {
            return new SequencePosition(value + 1);
        }

        @Override
        public int compareTo(SequencePosition anotherSequencePosition) {
            return Integer.compare(this.value, anotherSequencePosition.value);
        }

    }

    public record AppendCondition(Query failIfEventsMatch, SequencePosition after) {

        public static AppendConditionBuilder builder() {
            return new AppendConditionBuilder();
        }

        public static class AppendConditionBuilder {
            private Query failIfEventsMatch;
            private SequencePosition after;

            private AppendConditionBuilder() {
            }

            public FailIfEventsMatchAfterBuilder failIfEventsMatch(Query failIfEventsMatch) {
                this.failIfEventsMatch = failIfEventsMatch;
                return this.new FailIfEventsMatchAfterBuilder();
            }

            public class FailIfEventsMatchAfterBuilder {

                public AppendConditionBuilder after(int sequencePosition) {
                    AppendConditionBuilder.this.after = SequencePosition.of(sequencePosition);
                    return AppendConditionBuilder.this;
                }

                public AppendCondition build() {
                    return AppendConditionBuilder.this.build();
                }
            }

            public AppendCondition build() {
                if (this.failIfEventsMatch == null) {
                    throw new IllegalArgumentException("failIfEventsMatch must be set");
                }
                return new AppendCondition(this.failIfEventsMatch, this.after);
            }
        }
    }

    /**
     * @param startingPosition Start position, inclusive, possible range is [0, {@literal <last-position>}]
     * @param stopPosition     Stop position, exclusive, possible range is [0, {@literal <last-position+1>}]
     */
    public record ReadOptions(SequencePosition startingPosition, SequencePosition stopPosition) {

        public static ReadOptionsBuilder builder() {
            return new ReadOptionsBuilder();
        }

        public static class ReadOptionsBuilder {

            private SequencePosition startingPosition = SequencePosition.of(0);
            private SequencePosition stopPosition;

            private ReadOptionsBuilder() {
            }

            /**
             * @param startingPosition Start position, inclusive, possible range is [0, {@literal <last-position>}], Defaults to 0
             * @return
             */
            public ReadOptionsBuilder withStartingPosition(int startingPosition) {
                return withStartingPosition(SequencePosition.of(startingPosition));
            }

            /**
             * @param startingPosition Start position, inclusive, possible range is [0, {@literal <last-position>}], Defaults to 0
             * @return
             */
            public ReadOptionsBuilder withStartingPosition(SequencePosition startingPosition) {
                this.startingPosition = startingPosition;
                return this;
            }

            /**
             * @param stopPosition Stopping position, exclusive, possible range is [0, {@literal <last-position+1>}], Defaults to null (no stopping position)
             * @return
             */
            public ReadOptionsBuilder withStoppingPosition(int stopPosition) {
                return withStoppingPosition(SequencePosition.of(stopPosition));
            }

            /**
             * @param stopPosition Stopping position, exclusive, possible range is [0, {@literal <last-position+1>}], Defaults to null (no stopping position)
             * @return
             */
            public ReadOptionsBuilder withStoppingPosition(SequencePosition stopPosition) {
                this.stopPosition = stopPosition;
                return this;
            }

            public ReadOptions build() {
                return new ReadOptions(this.startingPosition, this.stopPosition);
            }

        }

    }

    public class AppendConditionNotSatisfied extends Exception {

        public AppendConditionNotSatisfied(String message) {
            super(message);
        }
    }

}
