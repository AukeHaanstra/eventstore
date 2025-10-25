package nl.pancompany.eventstore;

import nl.pancompany.eventstore.EventStore.Event;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class EventStoreDcbTest {

    record MyEvent(String id, String data) {

        public MyEvent(String data) {
            this(UUID.randomUUID().toString(), data);
        }
    }

    record MyOtherEvent(String id, String data) {
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
        assertThat(events).containsExactly(event);
    }

    @Test
    void notRetrievesEventNotMatchingTagQuery() {
        var myEvent = new MyEvent("data");
        Event event = new Event(myEvent, Set.of(Tag.of("something else")));

        eventStore.append(event);

        List<Event> events = eventStore.read(Query.taggedWith("test").build());
        assertThat(events).hasSize(0);
    }

    @Test
    void retrievesEventMatchingTypeQuery() {
        var myEvent = new MyEvent("data");
        Event event = new Event(myEvent);

        eventStore.append(event);

        List<Event> events = eventStore.read(Query.havingType(MyEvent.class).build());
        assertThat(events).containsExactly(event);
    }

    @Test
    void notRetrievesEventNotMatchingTypeQuery() {
        var myEvent = new MyEvent("data");
        Event event = new Event(myEvent);

        eventStore.append(event);

        List<Event> events = eventStore.read(Query.havingType(MyOtherEvent.class).build());
        assertThat(events).hasSize(0);
    }

    @Test
    void retrievesEventMatchingTagAndTypeQuery() {
        var myEvent = new MyEvent("data");
        Event event = new Event(myEvent, "MyEntity", "MyOtherEntity");

        eventStore.append(event);

        List<Event> events = eventStore.read(Query
                .taggedWith("MyEntity", "MyOtherEntity")
                .andHavingType(MyEvent.class));
        assertThat(events).containsExactly(event);
    }

    @Test
    void notRetrievesEventNotMatchingTagAndTypeQuery() {
        var myEvent = new MyEvent("data");
        Event event = new Event(myEvent, "MyEntity", "MyOtherEntity");

        eventStore.append(event);

        List<Event> events = eventStore.read(Query
                .taggedWith("MyEntity", "MyOtherEntity", "AbsentTag")
                .andHavingType(MyEvent.class));
        assertThat(events).hasSize(0);
    }

}


