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
            events.add(event);
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
                    for (Tag tag : queryItem.getTags()) {
                        queryItemSequencePositions.retainAll(tagPositions.get(tag));
                    }
                }
                if (!queryItem.isAllTypes()) {
                    Set<SequencePosition> queryItemTypePositions = new HashSet<>();
                    for (Type type : queryItem.getTypes()) {
                        queryItemTypePositions.addAll(typePositions.get(type));
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

    public record Event(Object payload) {

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

        @Override
        public int compareTo(SequencePosition anotherSequencePosition) {
            return Integer.compare(this.value, anotherSequencePosition.value);
        }

    }


}
