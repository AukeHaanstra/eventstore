package nl.pancompany.eventstore;

import nl.pancompany.eventstore.EventStore.Event;
import nl.pancompany.eventstore.EventStore.ReadOptions;
import nl.pancompany.eventstore.EventStore.SequencedEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
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

        List<SequencedEvent> sequencedEvents = eventStore.read(Query.taggedWith("test").build());
        assertThat(toEvents(sequencedEvents)).containsExactly(event);
    }

    @Test
    void notRetrievesEventNotMatchingTagQuery() {
        var myEvent = new MyEvent("data");
        Event event = new Event(myEvent, Set.of(Tag.of("something else")));

        eventStore.append(event);

        List<SequencedEvent> sequencedEvents = eventStore.read(Query.taggedWith("test").build());
        assertThat(sequencedEvents).hasSize(0);
    }

    @Test
    void retrievesEventMatchingTypeQuery() {
        var myEvent = new MyEvent("data");
        Event event = new Event(myEvent);

        eventStore.append(event);

        List<SequencedEvent> sequencedEvents = eventStore.read(Query.havingType(MyEvent.class).build());
        assertThat(toEvents(sequencedEvents)).containsExactly(event);
    }

    @Test
    void notRetrievesEventNotMatchingTypeQuery() {
        var myEvent = new MyEvent("data");
        Event event = new Event(myEvent);

        eventStore.append(event);

        List<SequencedEvent> sequencedEvents = eventStore.read(Query.havingType(MyOtherEvent.class).build());
        assertThat(sequencedEvents).hasSize(0);
    }

    @Test
    void retrievesEventMatchingTagAndTypeQuery() {
        var myEvent = new MyEvent("data");
        Event event = new Event(myEvent, "MyEntity", "MyOtherEntity");

        eventStore.append(event);

        List<SequencedEvent> sequencedEvents = eventStore.read(Query
                .taggedWith("MyEntity", "MyOtherEntity")
                .andHavingType(MyEvent.class));
        assertThat(toEvents(sequencedEvents)).containsExactly(event);
    }

    @Test
    void notRetrievesEventNotMatchingTagAndTypeQuery() {
        var myEvent = new MyEvent("data");
        Event event = new Event(myEvent, "MyEntity", "MyOtherEntity");

        eventStore.append(event);

        List<SequencedEvent> sequencedEvents = eventStore.read(Query
                .taggedWith("MyEntity", "MyOtherEntity", "AbsentTag")
                .andHavingType(MyEvent.class));
        assertThat(sequencedEvents).hasSize(0);
    }

    @Test
    void notRetrievesEventNotMatchingTagAndTypeQuery2() {
        var myEvent = new MyEvent("data");
        Event event = new Event(myEvent, "MyEntity", "MyOtherEntity");

        eventStore.append(event);

        List<SequencedEvent> sequencedEvents = eventStore.read(Query
                .taggedWith("MyEntity", "MyOtherEntity")
                .andHavingType(MyOtherEvent.class));
        assertThat(sequencedEvents).hasSize(0);
    }

    @Test
    void notRetrievesEventNotMatchingTagAndTypeQuery3() {
        var myEvent = new MyEvent("data");
        Event event = new Event(myEvent, "MyEntity", "MyOtherEntity");

        eventStore.append(event);

        List<SequencedEvent> sequencedEvents = eventStore.read(Query
                .taggedWith("AbsentTag")
                .andHavingType(MyEvent.class));
        assertThat(sequencedEvents).hasSize(0);
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

        List<SequencedEvent> sequencedEvents = eventStore.read(Query
                .taggedWith("MyEntity2", "MyOtherEntity2")
                .andHavingType(MyEvent.class));
        assertThat(toEvents(sequencedEvents)).containsExactly(event2);
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

        List<SequencedEvent> sequencedEvents = eventStore.read(Query
                .taggedWith("MyEntity1", "MyEntity2")
                .andHavingType(MyEvent.class));
        assertThat(toEvents(sequencedEvents)).containsExactly(event2);
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

        List<SequencedEvent> sequencedEvents = eventStore.read(Query
                .taggedWith("MyEntity1")
                .andHavingType(MyEvent.class));
        assertThat(toEvents(sequencedEvents)).containsExactly(event2);
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

        List<SequencedEvent> sequencedEvents = eventStore.read(Query
                .taggedWith("MyEntity2")
                .andHavingType(MyEvent.class, MyOtherEvent.class));
        assertThat(toEvents(sequencedEvents)).containsExactly(event2);
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

        List<SequencedEvent> sequencedEvents = eventStore.read(Query
                .taggedWith("MyEntity1")
                .andHavingType(MyEvent.class, MyOtherEvent.class));
        assertThat(toEvents(sequencedEvents)).containsExactly(event1, event2);
    }

    @Test
    void onlyReturnsEventsFromGivenStartPosition() {
        List<Event> appendedEvents = new ArrayList<>();
        for (int i = 0; i < 1000 ; i++) {
            Event event = new Event(new MyEvent("event" + i), "MyEntity1", "event" + i);
            appendedEvents.add(event);
            eventStore.append(event);
        }

        List<SequencedEvent> queriedEvents = eventStore.read(Query.of("MyEntity1", MyEvent.class), ReadOptions.builder()
                .withStartingPosition(400).build());

        assertThat(eventStore.read(Query.taggedWith("event0").build()).getFirst().position().value()).isEqualTo(0);
        assertThat(queriedEvents).hasSize(600);
        assertThat(queriedEvents.getFirst().position().value()).isEqualTo(400);
        assertThat(queriedEvents.getFirst().payload(MyEvent.class).data).isEqualTo("event400");
        assertThat(queriedEvents.getLast().position().value()).isEqualTo(999);
        assertThat(queriedEvents.getLast().payload(MyEvent.class).data).isEqualTo("event999");
        // All events filtered have a position >= 400
        assertThat(queriedEvents.stream().map(event -> event.position().value() >= 400).reduce(Boolean::logicalAnd).orElse(false)).isTrue();
    }

    private static List<Event> toEvents(List<SequencedEvent> sequencedEvents) {
        return sequencedEvents.stream().map(SequencedEvent::toEvent).toList();
    }

}


