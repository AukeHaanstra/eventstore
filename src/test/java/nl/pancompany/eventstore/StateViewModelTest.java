package nl.pancompany.eventstore;

import nl.pancompany.eventstore.StateManager.StateManagerOptimisticLockingException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
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
        myInitialEvent = new MyInitialEvent(MY_ENTITY_ID, "0");
        myEvent = new MyEvent(MY_ENTITY_ID, "1");
        myOtherEvent = new MyOtherEvent(MY_ENTITY_ID, "2");
        myNewEvent = new MyNewEvent(MY_ENTITY_ID, "3");
        event0 = Event.of(myInitialEvent, Tag.of("MyEntity", MY_ENTITY_ID));
        event1 = Event.of(myEvent, Tag.of("MyEntity", MY_ENTITY_ID));
        event2 = Event.of(myOtherEvent, Tag.of("MyEntity", MY_ENTITY_ID));
        event3 = Event.of(myNewEvent, Tag.of("MyEntity", MY_ENTITY_ID));
    }

    @AfterEach
    void tearDown() {
        eventStore.close();
    }

    @Test
    void rehydratesStateViewModel() {
        // Arrange
        eventStore.append(event0, event1,event2);
        Query query = Query
                .taggedWith(Tag.of("MyEntity", MY_ENTITY_ID))
                .andHavingType(MyInitialEvent.class, MyEvent.class, MyOtherEvent.class, MyNewEvent.class);
        List<SequencedEvent> sequencedEvents = eventStore.read(query);
        assertThat(toEvents(sequencedEvents)).containsExactly(event0, event1, event2);

        // Act (just like in a command handler)
        StateManager<MyState> stateManager = eventStore.loadState(MyState.class, query);

        MyState state = stateManager.getState().get();
        // < business rules validation and decision-making (state change or automation) >

        stateManager.apply(myNewEvent, Tag.of("MyEntity", MY_ENTITY_ID), Type.of(MyNewEvent.class));

        // Assert
        assertThat(state.myHandledEvents).containsExactly(myInitialEvent, myEvent, myOtherEvent, myNewEvent);
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
        List<SequencedEvent> sequencedEvents = eventStore.read(query);
        assertThat(toEvents(sequencedEvents)).containsExactly(event0, event1, event2);

        // Act (just like in a command handler)
        StateManager<MyStateWithoutStateConstructor> stateManager = eventStore.loadState(MyStateWithoutStateConstructor.class, query);

        MyStateWithoutStateConstructor state = stateManager.getState().get();
        // < business rules validation and decision-making (state change or automation) >

        stateManager.apply(myNewEvent, Tag.of("MyEntity", MY_ENTITY_ID), Type.of(MyNewEvent.class));

        // Assert
        assertThat(state.myHandledEvents).containsExactly(myInitialEvent, myEvent, myOtherEvent, myNewEvent);
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
        List<SequencedEvent> sequencedEvents = eventStore.read(query);
        assertThat(toEvents(sequencedEvents)).containsExactly(event0, event1, event2);

        // Act (just like in a command handler)
        StateManager<MyStateWithoutStateConstructor> stateManager = eventStore.loadState(new MyStateWithoutStateConstructor(), query);

        MyStateWithoutStateConstructor state = stateManager.getState().get();
        // < business rules validation and decision-making (state change or automation) >

        stateManager.apply(myNewEvent, Tag.of("MyEntity", MY_ENTITY_ID), Type.of(MyNewEvent.class));

        // Assert
        assertThat(state.myHandledEvents).containsExactly(myInitialEvent, myEvent, myOtherEvent, myNewEvent);
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
        List<SequencedEvent> sequencedEvents = eventStore.read(query);
        assertThat(toEvents(sequencedEvents)).containsExactly(event0, event1, event2);

        // Act (just like in a command handler)
        StateManager<MyStateWithoutStateConstructor> stateManager = eventStore.loadState(new MyStateWithoutStateConstructor(), query);

        MyStateWithoutStateConstructor state = stateManager.getState().get();
        // < business rules validation and decision-making (state change or automation) >

        var concurrentlySavedMyEvent = new MyEvent(MY_ENTITY_ID, "4");
        Event concurrentlySavedEvent = Event.of(concurrentlySavedMyEvent, Tag.of("MyEntity", MY_ENTITY_ID));
        eventStore.append(concurrentlySavedEvent);

        assertThatThrownBy(() -> stateManager.apply(myNewEvent, Tag.of("MyEntity", MY_ENTITY_ID), Type.of(MyNewEvent.class)))
                .isInstanceOf(StateManagerOptimisticLockingException.class);
        // Assert
        assertThat(state.myHandledEvents).containsExactly(myInitialEvent, myEvent, myOtherEvent, myNewEvent);
        sequencedEvents = eventStore.read(query);
        assertThat(toEvents(sequencedEvents)).containsExactly(event0, event1, event2, concurrentlySavedEvent);
    }

    @Test
    void throwsException_WhenStateClassHasInvalidConstructor() {
        assertThatThrownBy(() -> eventStore.loadState(InvalidStateClass.class, Query.all()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void createsUninitializedStateForStateConstructorAndNoEvents() {
        // Arrange
        eventStore.append();
        Query query = Query
                .taggedWith(Tag.of("MyEntity", MY_ENTITY_ID))
                .andHavingType(MyInitialEvent.class, MyEvent.class, MyOtherEvent.class, MyNewEvent.class);

        // Act (just like in a command handler)
        StateManager<MyState> stateManager = eventStore.loadState(MyState.class, query);

        Optional<MyState> optionalEntity = stateManager.getState();
        // < business rules validation and decision-making (state change or automation) >

        assertThat(optionalEntity).isEmpty();
    }

    @Test
    void applyingRegularEventToUninitializedStateIsNotPossible() {
        // Arrange
        Query query = Query
                .taggedWith(Tag.of("MyEntity", MY_ENTITY_ID))
                .andHavingType(MyInitialEvent.class, MyEvent.class, MyOtherEvent.class, MyNewEvent.class);
        List<SequencedEvent> sequencedEvents = eventStore.read(query);
        assertThat(sequencedEvents).isEmpty();

        // Act (just like in a command handler)
        StateManager<MyState> stateManager = eventStore.loadState(MyState.class, query);


        // < business rules validation and decision-making (state change or automation) >

        assertThatThrownBy(() ->  stateManager.apply(myEvent, Tag.of("MyEntity", MY_ENTITY_ID), Type.of(MyEvent.class)))
                .isInstanceOf(StateConstructionFailedException.class);
    }

    @Test
    void applyingCreatorEventToUninitializedStateIsPossible() {
        // Arrange
        Query query = Query
                .taggedWith(Tag.of("MyEntity", MY_ENTITY_ID))
                .andHavingType(MyInitialEvent.class, MyEvent.class, MyOtherEvent.class, MyNewEvent.class);
        List<SequencedEvent> sequencedEvents = eventStore.read(query);
        assertThat(sequencedEvents).isEmpty();

        // Act (just like in a command handler)
        StateManager<MyState> stateManager = eventStore.loadState(MyState.class, query);

        // < business rules validation and decision-making (state change or automation) >

        stateManager.apply(myInitialEvent, Tag.of("MyEntity", MY_ENTITY_ID), Type.of(MyInitialEvent.class));
        stateManager.apply(myEvent, Tag.of("MyEntity", MY_ENTITY_ID), Type.of(MyEvent.class));

        // Assert
        MyState state = stateManager.getState().get();
        assertThat(state.myHandledEvents).containsExactly(myInitialEvent, myEvent);
        sequencedEvents = eventStore.read(query);
        assertThat(toEvents(sequencedEvents)).containsExactly(event0, event1);
    }

    private static List<Event> toEvents(List<SequencedEvent> sequencedEvents) {
        return sequencedEvents.stream().map(SequencedEvent::toEvent).toList();
    }

    private static class InvalidStateClass {

        private final List<Object> myHandledEvents = new CopyOnWriteArrayList<>();

        @StateCreator
        private InvalidStateClass(MyInitialEvent event, MyOtherEvent myOtherEvent) {
            myHandledEvents.add(event);
        }

    }

    private static class MyState {

        private final List<Object> myHandledEvents = new CopyOnWriteArrayList<>();

        @StateCreator
        private MyState(MyInitialEvent event) {
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

    private static class MyStateWithoutStateConstructor {

        private final List<Object> myHandledEvents = new CopyOnWriteArrayList<>();

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
