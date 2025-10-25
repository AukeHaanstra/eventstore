package nl.pancompany.eventstore;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class EventStore {

    private final List<Event> events = new ArrayList<>();
    private final HashMap<Tag, Set<SequencePosition>> tagPositions = new HashMap<>();
    private final HashMap<Type, Set<SequencePosition>> typePositions = new HashMap<>();
    private final Set<SequencePosition> allSequencePositions = new HashSet<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    /**
     * Contract: event payload must always be immutable to guarantee immutability of events in the event store
     *
     * @param event The event instance that wraps a payload (the raw event).
     */
    public void append(Event event/*, AppendCondition appendCondition*/) {
        try {
            writeLock.lock();
            SequencePosition insertPosition = SequencePosition.of(events.size());
            events.add(event);
            allSequencePositions.add(insertPosition);
            for (Tag tag : event.tags) {
                tagPositions.computeIfAbsent(tag, k -> new HashSet<>()).add(insertPosition);
            }
            typePositions.computeIfAbsent(event.type, k -> new HashSet<>()).add(insertPosition);
        } finally {
            writeLock.unlock();
        }

    }

    public List<Event> read(Query query/*, ReadOptions options*/) {
        try {
            readLock.lock();
            Set<SequencePosition> querySequencePositions = new HashSet<>();
            for (QueryItem queryItem : query.getQueryItems()) {
                if (queryItem.isAll()) {
                    return new ArrayList<>(events);
                }
                Set<SequencePosition> queryItemSequencePositions = new HashSet<>(allSequencePositions);
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
            return querySequencePositions.stream()
                    .sorted()
                    .map(position -> events.get(position.value))
                    .toList();
        } finally {
            readLock.unlock();
        }
    }



    public record Event(Object payload, Set<Tag> tags, Type type) {

        public Event(Object payload) {
            this(payload, Collections.emptySet(), getName(payload));
        }

        public Event(Object payload, Type type) {
            this(payload, Collections.emptySet(), type);
        }

        public Event(Object payload, Set<Tag> tags) {
            this(payload, tags, getName(payload));
        }

        static Type getName(Object payload) {
            return Type.of(payload.getClass());
        }

        @SuppressWarnings("unchecked")
        public <T> T payload(Class<T> clazz) {
            if (!clazz.isAssignableFrom(payload.getClass())) {
                throw new IllegalArgumentException("Payload is not assignable to " + clazz);
            }
            return (T) payload;
        }
    }

    public record AppendCondition(Query failIfEventsMatch, SequencePosition after) {

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


}
