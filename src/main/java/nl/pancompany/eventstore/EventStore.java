package nl.pancompany.eventstore;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class EventStore {

    private final List<SequencedEvent> storedEvents = new ArrayList<>();
    private final HashMap<Tag, Set<SequencePosition>> tagPositions = new HashMap<>();
    private final HashMap<Type, Set<SequencePosition>> typePositions = new HashMap<>();
    private final Set<SequencePosition> allSequencePositions = new HashSet<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    /**
     * Contract: event payload must always be immutable to guarantee immutability of events in the event store
     *
     * @param events The event instances that wrap a payload (the raw event).
     */
    public void append(Event... events) {
        try {
            append(List.of(events), null);
        } catch (AppendConditionNotSatisfied e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Contract: event payload must always be immutable to guarantee immutability of events in the event store
     *
     * @param events The event instances that wrap a payload (the raw event).
     */
    public void append(List<Event> events) {
        try {
            append(events, null);
        } catch (AppendConditionNotSatisfied e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Contract: event payload must always be immutable to guarantee immutability of events in the event store
     *
     * @param event The event instances that wrap a payload (the raw event).
     */
    public void append(Event event, AppendCondition appendCondition) throws AppendConditionNotSatisfied {
        append(List.of(event), appendCondition);
    }

    /**
     * Contract: event payload must always be immutable to guarantee immutability of events in the event store
     *
     * @param events The event instances that wrap a payload (the raw event).
     */
    public void append(List<Event> events, AppendCondition appendCondition) throws AppendConditionNotSatisfied {
        try {
            writeLock.lock();
            if (appendCondition != null) {
                checkWhetherEventsFailAppendCondition(appendCondition);
            }
            for (Event event : events) {
                SequencePosition insertPosition = SequencePosition.of(storedEvents.size());
                storedEvents.add(new SequencedEvent(event, insertPosition));
                allSequencePositions.add(insertPosition);
                for (Tag tag : event.tags) {
                    tagPositions.computeIfAbsent(tag, k -> new HashSet<>()).add(insertPosition);
                }
                typePositions.computeIfAbsent(event.type, k -> new HashSet<>()).add(insertPosition);
            }
        } finally {
            writeLock.unlock();
        }

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
        Set<SequencePosition> sequencePositionsFromStart = allSequencePositions.stream().filter(options == null ?
                        position -> true :
                        position -> position.value >= options.startingPosition.value)
                .collect(Collectors.toSet());
        Set<SequencePosition> querySequencePositions = new HashSet<>();
        for (QueryItem queryItem : query.getQueryItems()) {
            if (queryItem.isAll()) {
                return sequencePositionsToEvents(sequencePositionsFromStart);
            }
            Set<SequencePosition> queryItemSequencePositions = new HashSet<>(sequencePositionsFromStart);
            if (!queryItem.isAllTags()) {
                for (Tag tag : queryItem.tags()) {
                    queryItemSequencePositions.retainAll(tagPositions.computeIfAbsent(tag, t -> new HashSet<>()));
                }
            }
            if (!queryItem.isAllTypes()) {
                Set<SequencePosition> queryItemTypePositions = new HashSet<>();
                for (Type type : queryItem.types()) {
                    queryItemTypePositions.addAll(typePositions.computeIfAbsent(type, t -> new HashSet<>()));
                }
                queryItemSequencePositions.retainAll(queryItemTypePositions);
            }
            querySequencePositions.addAll(queryItemSequencePositions);
        }
        return sequencePositionsToEvents(querySequencePositions);
    }

    private List<SequencedEvent> sequencePositionsToEvents(Set<SequencePosition> querySequencePositions) {
        return querySequencePositions.stream()
                .sorted()
                .map(position -> storedEvents.get(position.value))
                .toList();
    }

    public record Event(Object payload, Set<Tag> tags, Type type) {

        public Event(Object payload) {
            this(payload, Collections.emptySet(), getName(payload));
        }

        public Event(Object payload, Type type) {
            this(payload, Collections.emptySet(), type);
        }

        public Event(Object payload, Tag... tags) {
            this(payload, Set.of(tags), getName(payload));
        }

        public Event(Object payload, Set<Tag> tags) {
            this(payload, tags, getName(payload));
        }

        public Event(Object payload, String... tags) {
            this(payload, Arrays.stream(tags).map(Tag::of).collect(Collectors.toSet()), getName(payload));
        }

        public Event(Object payload, Type type, Tag... tags) {
            this(payload, Set.of(tags), type);
        }

        public Event(Object payload, Type type, Set<Tag> tags) {
            this(payload, tags, type);
        }

        public Event(Object payload, Type type, String... tags) {
            this(payload, Arrays.stream(tags).map(Tag::of).collect(Collectors.toSet()), type);
        }

        public static Event of(Object payload) {
            return new Event(payload, Collections.emptySet(), getName(payload));
        }

        public static Event of(Object payload, Type type) {
            return new Event(payload, Collections.emptySet(), type);
        }

        public static Event of(Object payload, Tag... tags) {
            return new Event(payload, Set.of(tags), getName(payload));
        }

        public static Event of(Object payload, Set<Tag> tags) {
            return new Event(payload, tags, getName(payload));
        }

        public static Event of(Object payload, String... tags) {
            return new Event(payload, Arrays.stream(tags).map(Tag::of).collect(Collectors.toSet()), getName(payload));
        }

        public static Event of(Object payload, Type type, Tag... tags) {
            return new Event(payload, Set.of(tags), type);
        }

        public static Event of(Object payload, Type type, Set<Tag> tags) {
            return new Event(payload, tags, type);
        }

        public static Event of(Object payload, Type type, String... tags) {
            return new Event(payload, Arrays.stream(tags).map(Tag::of).collect(Collectors.toSet()), type);
        }

        private static Type getName(Object payload) {
            return Type.of(payload.getClass());
        }

    }

    public record SequencedEvent(Object payload, Set<Tag> tags, Type type, SequencePosition position) {

        public SequencedEvent(Event event, SequencePosition position) {
            this(event.payload, event.tags, event.type, position);
        }

        @SuppressWarnings("unchecked")
        public <T> T payload(Class<T> clazz) {
            if (!clazz.isAssignableFrom(payload.getClass())) {
                throw new IllegalArgumentException("Payload is not assignable to " + clazz);
            }
            return (T) payload;
        }

        /**
         * Beware, this operation causes a loss of sequence position information.
         * @return The event corresponding to this sequenced event
         */
        public Event toEvent() {
            return new Event(payload(), tags, type);
        }
    }

    public record SequencePosition(int value) implements Comparable<SequencePosition> {

        public static SequencePosition of(int i) {
            return new SequencePosition(i);
        }

        public SequencePosition incrementAndGet() {
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
     *
     * @param startingPosition Start position, inclusive, possible range is [0, {@literal <last-position>}]
     */
    public record ReadOptions(SequencePosition startingPosition) {

        public static ReadOptionsBuilder builder() {
            return new ReadOptionsBuilder();
        }

        public static class ReadOptionsBuilder {

            private SequencePosition startingPosition;

            private ReadOptionsBuilder() {
            }

            public ReadOptionsBuilder withStartingPosition(int startingPosition) {
                this.startingPosition = SequencePosition.of(startingPosition);
                return this;
            }

            public ReadOptions build() {
                return new ReadOptions(this.startingPosition);
            }

        }

    }

    public class AppendConditionNotSatisfied extends Exception {

        public AppendConditionNotSatisfied(String message) {
            super(message);
        }
    }
}
