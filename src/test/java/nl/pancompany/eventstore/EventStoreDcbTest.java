package nl.pancompany.eventstore;

import nl.pancompany.eventstore.EventStore.Event;
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

        public MyOtherEvent(String data) {
            this(UUID.randomUUID().toString(), data);
        }
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

    @Test
    void notRetrievesEventNotMatchingTagAndTypeQuery2() {
        var myEvent = new MyEvent("data");
        Event event = new Event(myEvent, "MyEntity", "MyOtherEntity");

        eventStore.append(event);

        List<Event> events = eventStore.read(Query
                .taggedWith("MyEntity", "MyOtherEntity")
                .andHavingType(MyOtherEvent.class));
        assertThat(events).hasSize(0);
    }

    @Test
    void notRetrievesEventNotMatchingTagAndTypeQuery3() {
        var myEvent = new MyEvent("data");
        Event event = new Event(myEvent, "MyEntity", "MyOtherEntity");

        eventStore.append(event);

        List<Event> events = eventStore.read(Query
                .taggedWith("AbsentTag")
                .andHavingType(MyEvent.class));
        assertThat(events).hasSize(0);
    }

    @Test
    void onlyRetrievesEventsMatchingTagAndTypeQuery() {
        var myEvent = new MyEvent("data");
        Event event1 = new Event(myEvent, "MyEntity1", "MyOtherEntity1");
        Event event2 = new Event(myEvent, "MyEntity2", "MyOtherEntity2");
        Event event3 = new Event(myEvent, "MyEntity3", "MyOtherEntity3");

        eventStore.append(event1);
        eventStore.append(event2);
        eventStore.append(event3);

        List<Event> events = eventStore.read(Query
                .taggedWith("MyEntity2", "MyOtherEntity2")
                .andHavingType(MyEvent.class));
        assertThat(events).containsExactly(event2);
    }

    @Test
    void onlyRetrievesEventsMatchingTagAndTypeQuery2() {
        var myEvent = new MyEvent("data");
        Event event1 = new Event(myEvent, "MyEntity1");
        Event event2 = new Event(myEvent, "MyEntity1", "MyEntity2");
        Event event3 = new Event(myEvent, "MyEntity3");

        eventStore.append(event1);
        eventStore.append(event2);
        eventStore.append(event3);

        List<Event> events = eventStore.read(Query
                .taggedWith("MyEntity1", "MyEntity2")
                .andHavingType(MyEvent.class));
        assertThat(events).containsExactly(event2);
    }

    @Test
    void onlyRetrievesEventsMatchingTagAndTypeQuery3() {
        var myEvent = new MyEvent("data");
        var myOtherEvent = new MyOtherEvent("data");
        Event event1 = new Event(myOtherEvent, "MyEntity1");
        Event event2 = new Event(myEvent, "MyEntity1");
        Event event3 = new Event(myOtherEvent, "MyEntity1");

        eventStore.append(event1);
        eventStore.append(event2);
        eventStore.append(event3);

        List<Event> events = eventStore.read(Query
                .taggedWith("MyEntity1")
                .andHavingType(MyEvent.class));
        assertThat(events).containsExactly(event2);
    }

    @Test
    void onlyRetrievesEventsMatchingTagAndTypeQuery4() {
        var myEvent = new MyEvent("data");
        var myOtherEvent = new MyOtherEvent("data");
        Event event1 = new Event(myOtherEvent, "MyEntity1");
        Event event2 = new Event(myEvent, "MyEntity2");
        Event event3 = new Event(myOtherEvent, "MyEntity3");

        eventStore.append(event1);
        eventStore.append(event2);
        eventStore.append(event3);

        List<Event> events = eventStore.read(Query
                .taggedWith("MyEntity2")
                .andHavingType(MyEvent.class, MyOtherEvent.class));
        assertThat(events).containsExactly(event2);
    }

    @Test
    void onlyRetrievesEventsMatchingTagAndTypeQuery5() {
        var myEvent = new MyEvent("data");
        var myOtherEvent = new MyOtherEvent("data");
        Event event1 = new Event(myOtherEvent, "MyEntity1");
        Event event2 = new Event(myEvent, "MyEntity1");
        Event event3 = new Event(myOtherEvent, "MyEntity3");

        eventStore.append(event1);
        eventStore.append(event2);
        eventStore.append(event3);

        List<Event> events = eventStore.read(Query
                .taggedWith("MyEntity1")
                .andHavingType(MyEvent.class, MyOtherEvent.class));
        assertThat(events).containsExactly(event1, event2);
    }

}


