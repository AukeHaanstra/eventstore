package nl.pancompany.eventstore;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class EventStore {

    private final List<Event> events = new ArrayList<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    /**
     * Contract: event payload must always be immutable to guarantee immutability of events in the event store
     *
     * @param event The event instance that wraps a payload (the raw event).
     */
    public void append(Event event) {
        try {
            writeLock.lock();
            events.add(event);
        } finally {
            writeLock.unlock();
        }

    }

    public List<Event> read() {
        try {
            readLock.lock();
            return new ArrayList<>(events);
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


}
