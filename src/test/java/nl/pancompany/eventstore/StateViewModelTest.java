package nl.pancompany.eventstore;

import nl.pancompany.eventstore.EventStore.Event;
import nl.pancompany.eventstore.StateManager.State;
import nl.pancompany.eventstore.StateManager.StateManagerOptimisticLockingException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// TODO: Also test error situations and special setups
public class StateViewModelTest {

    private EventStore eventStore;
    private static final String MY_ENTITY_ID = UUID.randomUUID().toString();
    MyInitialEvent myInitialEvent;
    MyEvent myEvent;
    MyOtherEvent myOtherEvent;
    MyNewEvent myNewEvent;
    Event event0, event1, event2, event3;

    @BeforeEach
    void setUp() {
        eventStore = new EventStore();
        MyEntity.myHandledEvents.clear();
        MyEntityWithoutStateConstructor.myHandledEvents.clear();
        myInitialEvent = new MyInitialEvent(MY_ENTITY_ID, "0");
        myEvent = new MyEvent(MY_ENTITY_ID, "1");
        myOtherEvent = new MyOtherEvent(MY_ENTITY_ID, "2");
        myNewEvent = new MyNewEvent(MY_ENTITY_ID, "3");
        event0 = Event.of(myInitialEvent, Tag.of("MyEntity", MY_ENTITY_ID));
        event1 = Event.of(myEvent, Tag.of("MyEntity", MY_ENTITY_ID));
        event2 = Event.of(myOtherEvent, Tag.of("MyEntity", MY_ENTITY_ID));
        event3 = Event.of(myNewEvent, Tag.of("MyEntity", MY_ENTITY_ID));
    }

    @Test
    void rehydratesStateViewModel() {
        // Arrange
        eventStore.append(event0, event1,event2);
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
        eventStore.append(event0, event1,event2);
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
        eventStore.append(event0, event1,event2);
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

    @Test
    void thowsOptimisticLockingException_AndNotAppliesStateChange_WhenQueryResultsGetModifiedDuringStateChange() {
        // Arrange
        eventStore.append(event0, event1, event2);
        Query query = Query
                .taggedWith(Tag.of("MyEntity", MY_ENTITY_ID))
                .andHavingType(MyInitialEvent.class, MyEvent.class, MyOtherEvent.class, MyNewEvent.class);
        List<EventStore.SequencedEvent> sequencedEvents = eventStore.read(query);
        assertThat(toEvents(sequencedEvents)).containsExactly(event0, event1, event2);

        // Act (just like in a command handler)
        StateManager stateManager = eventStore.getStateManager();
        State state = stateManager.load(new MyEntityWithoutStateConstructor(), query);

        // < business rules validation and decision-making (state change or automation) >

        var concurrentlySavedMyEvent = new MyEvent(MY_ENTITY_ID, "4");
        Event concurrentlySavedEvent = Event.of(concurrentlySavedMyEvent, Tag.of("MyEntity", MY_ENTITY_ID));
        eventStore.append(concurrentlySavedEvent);

        assertThatThrownBy(() -> state.apply(myNewEvent, Tag.of("MyEntity", MY_ENTITY_ID), Type.of(MyNewEvent.class)))
                .isInstanceOf(StateManagerOptimisticLockingException.class);
        // Assert
        assertThat(MyEntityWithoutStateConstructor.myHandledEvents).containsExactly(myInitialEvent, myEvent, myOtherEvent, myNewEvent);
        sequencedEvents = eventStore.read(query);
        assertThat(toEvents(sequencedEvents)).containsExactly(event0, event1, event2, concurrentlySavedEvent);
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
