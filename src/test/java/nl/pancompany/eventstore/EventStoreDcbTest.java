package nl.pancompany.eventstore;

import nl.pancompany.eventstore.EventStore.Event;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

public class EventStoreDcbTest {

    record MyEvent(String data) {
    }

    EventStore eventStore;

    @BeforeEach
    void setUp() {
        eventStore = new EventStore();
    }

    @Test
    void retrievesEventMatchingTagQuery() {
        var myEvent = new MyEvent("data");
        Event event = new Event(myEvent, Set.of(Tag.of("test")));

        eventStore.append(event);

        List<Event> events = eventStore.read(Query.taggedWith("test").build());
        Assertions.assertThat(events).containsExactly(event);
    }

    @Test
    void notRetrievesEventNotMatchingTagQuery() {
        var myEvent = new MyEvent("data");
        Event event = new Event(myEvent, Set.of(Tag.of("something else")));

        eventStore.append(event);

        List<Event> events = eventStore.read(Query.taggedWith("test").build());
        Assertions.assertThat(events).hasSize(0);
    }

    @Test
    void retrievesEventMatchingTypeQuery() {
        var myEvent = new MyEvent("data");
        Event event = new Event(myEvent);

        eventStore.append(event);

        List<Event> events = eventStore.read(Query.havingType(MyEvent.class).build());
        Assertions.assertThat(events).containsExactly(event);
    }

}


