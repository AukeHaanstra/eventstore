package nl.pancompany.eventstore;

import nl.pancompany.eventstore.EventStore.Event;
import nl.pancompany.eventstore.StateManager.State;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

// TODO: Also test error situations and special setups
public class StateViewModelTest {

    private EventStore eventStore;
    private static final String MY_ENTITY_ID = UUID.randomUUID().toString();

    @BeforeEach
    void setUp() {
        eventStore = new EventStore();
        MyEntity.myHandledEvents.clear();
        MyEntityWithoutStateConstructor.myHandledEvents.clear();
    }

    @Test
    void rehydratesStateViewModel() {
        // Arrange
        var myInitialEvent = new MyInitialEvent(MY_ENTITY_ID, "0");
        var myEvent = new MyEvent(MY_ENTITY_ID, "1");
        var myOtherEvent = new MyOtherEvent(MY_ENTITY_ID, "2");
        var myNewEvent = new MyNewEvent(MY_ENTITY_ID, "3");
        Event event0 = Event.of(myInitialEvent, Tag.of("MyEntity", MY_ENTITY_ID));
        Event event1 = Event.of(myEvent, Tag.of("MyEntity", MY_ENTITY_ID));
        Event event2 = Event.of(myOtherEvent, Tag.of("MyEntity", MY_ENTITY_ID));
        Event event3 = Event.of(myNewEvent, Tag.of("MyEntity", MY_ENTITY_ID));

        // Arrange
        eventStore.append(event0);
        eventStore.append(event1);
        eventStore.append(event2);
        Query query = Query
                .taggedWith(Tag.of("MyEntity", MY_ENTITY_ID))
                .andHavingType(MyInitialEvent.class, MyEvent.class, MyOtherEvent.class, MyNewEvent.class);
        List<EventStore.SequencedEvent> sequencedEvents = eventStore.read(query);
        assertThat(toEvents(sequencedEvents)).containsExactly(event0, event1, event2);

        // Act (just like in a command handler)
        StateManager stateManager = eventStore.getStateManager();
        State state = stateManager.load(MyEntity.class, query);

        // < business rules validation and decision-making (state change or automation) >

        state.apply(myNewEvent, Tag.of("MyEntity", MY_ENTITY_ID), Type.of(MyNewEvent.class));

        // Assert
        assertThat(MyEntity.myHandledEvents).containsExactly(myInitialEvent, myEvent, myOtherEvent, myNewEvent);
        sequencedEvents = eventStore.read(query);
        assertThat(toEvents(sequencedEvents)).containsExactly(event0, event1, event2, event3);
    }

    @Test
    void rehydratesStateViewModelWithoutStateConstructor() {
        // Arrange
        var myInitialEvent = new MyInitialEvent(MY_ENTITY_ID, "0");
        var myEvent = new MyEvent(MY_ENTITY_ID, "1");
        var myOtherEvent = new MyOtherEvent(MY_ENTITY_ID, "2");
        var myNewEvent = new MyNewEvent(MY_ENTITY_ID, "3");
        Event event0 = Event.of(myInitialEvent, Tag.of("MyEntity", MY_ENTITY_ID));
        Event event1 = Event.of(myEvent, Tag.of("MyEntity", MY_ENTITY_ID));
        Event event2 = Event.of(myOtherEvent, Tag.of("MyEntity", MY_ENTITY_ID));
        Event event3 = Event.of(myNewEvent, Tag.of("MyEntity", MY_ENTITY_ID));

        // Arrange
        eventStore.append(event0);
        eventStore.append(event1);
        eventStore.append(event2);
        Query query = Query
                .taggedWith(Tag.of("MyEntity", MY_ENTITY_ID))
                .andHavingType(MyInitialEvent.class, MyEvent.class, MyOtherEvent.class, MyNewEvent.class);
        List<EventStore.SequencedEvent> sequencedEvents = eventStore.read(query);
        assertThat(toEvents(sequencedEvents)).containsExactly(event0, event1, event2);

        // Act (just like in a command handler)
        StateManager stateManager = eventStore.getStateManager();
        State state = stateManager.load(MyEntityWithoutStateConstructor.class, query);

        // < business rules validation and decision-making (state change or automation) >

        state.apply(myNewEvent, Tag.of("MyEntity", MY_ENTITY_ID), Type.of(MyNewEvent.class));

        // Assert
        assertThat(MyEntityWithoutStateConstructor.myHandledEvents).containsExactly(myInitialEvent, myEvent, myOtherEvent, myNewEvent);
        sequencedEvents = eventStore.read(query);
        assertThat(toEvents(sequencedEvents)).containsExactly(event0, event1, event2, event3);
    }

    @Test
    void rehydratesStateViewModelWithPreInstantiatedState() {
        // Arrange
        var myInitialEvent = new MyInitialEvent(MY_ENTITY_ID, "0");
        var myEvent = new MyEvent(MY_ENTITY_ID, "1");
        var myOtherEvent = new MyOtherEvent(MY_ENTITY_ID, "2");
        var myNewEvent = new MyNewEvent(MY_ENTITY_ID, "3");
        Event event0 = Event.of(myInitialEvent, Tag.of("MyEntity", MY_ENTITY_ID));
        Event event1 = Event.of(myEvent, Tag.of("MyEntity", MY_ENTITY_ID));
        Event event2 = Event.of(myOtherEvent, Tag.of("MyEntity", MY_ENTITY_ID));
        Event event3 = Event.of(myNewEvent, Tag.of("MyEntity", MY_ENTITY_ID));

        // Arrange
        eventStore.append(event0);
        eventStore.append(event1);
        eventStore.append(event2);
        Query query = Query
                .taggedWith(Tag.of("MyEntity", MY_ENTITY_ID))
                .andHavingType(MyInitialEvent.class, MyEvent.class, MyOtherEvent.class, MyNewEvent.class);
        List<EventStore.SequencedEvent> sequencedEvents = eventStore.read(query);
        assertThat(toEvents(sequencedEvents)).containsExactly(event0, event1, event2);

        // Act (just like in a command handler)
        StateManager stateManager = eventStore.getStateManager();
        State state = stateManager.load(new MyEntityWithoutStateConstructor(), query);

        // < business rules validation and decision-making (state change or automation) >

        state.apply(myNewEvent, Tag.of("MyEntity", MY_ENTITY_ID), Type.of(MyNewEvent.class));

        // Assert
        assertThat(MyEntityWithoutStateConstructor.myHandledEvents).containsExactly(myInitialEvent, myEvent, myOtherEvent, myNewEvent);
        sequencedEvents = eventStore.read(query);
        assertThat(toEvents(sequencedEvents)).containsExactly(event0, event1, event2, event3);
    }

    private static List<Event> toEvents(List<EventStore.SequencedEvent> sequencedEvents) {
        return sequencedEvents.stream().map(EventStore.SequencedEvent::toEvent).toList();
    }

    private static class MyEntity {

        private static List<Object> myHandledEvents = new CopyOnWriteArrayList<>();

        @StateConstructor
        private MyEntity(MyInitialEvent event) {
            myHandledEvents.add(event);
        }

        @EventSourced
        private void handle(MyEvent event) {
            myHandledEvents.add(event);
        }

        @EventSourced
        private void handle(MyOtherEvent myOtherEvent) {
            myHandledEvents.add(myOtherEvent);
        }

        @EventSourced
        private void handle(MyNewEvent myNewEvent) {
            myHandledEvents.add(myNewEvent);
        }

    }

    private static class MyEntityWithoutStateConstructor {

        private static List<Object> myHandledEvents = new CopyOnWriteArrayList<>();

        @EventSourced
        private void handle(MyInitialEvent event) {
            myHandledEvents.add(event);
        }

        @EventSourced
        private void handle(MyEvent event) {
            myHandledEvents.add(event);
        }

        @EventSourced
        private void handle(MyOtherEvent myOtherEvent) {
            myHandledEvents.add(myOtherEvent);
        }

        @EventSourced
        private void handle(MyNewEvent myNewEvent) {
            myHandledEvents.add(myNewEvent);
        }

    }

    record MyInitialEvent(String id, String data) {

    }

    record MyEvent(String id, String data) {

    }

    record MyOtherEvent(String id, String data) {

    }

    record MyNewEvent(String id, String data) {

    }
}
