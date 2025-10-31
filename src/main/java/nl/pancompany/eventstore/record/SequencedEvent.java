package nl.pancompany.eventstore.record;

import nl.pancompany.eventstore.EventStore;
import nl.pancompany.eventstore.query.Tag;
import nl.pancompany.eventstore.query.Type;

import java.util.Set;

public record SequencedEvent(Object payload, Set<Tag> tags, Type type, EventStore.SequencePosition position) {

    public SequencedEvent(Event event, EventStore.SequencePosition position) {
        this(event.payload(), event.tags(), event.type(), position);
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
     *
     * @return The event corresponding to this sequenced event
     */
    public Event toEvent() {
        return new Event(payload(), tags, type);
    }
}
