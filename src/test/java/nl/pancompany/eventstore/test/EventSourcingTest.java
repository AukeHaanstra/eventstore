package nl.pancompany.eventstore.test;

import nl.pancompany.eventstore.EventStore;
import nl.pancompany.eventstore.StateManager;
import nl.pancompany.eventstore.StateManager.StateManagerOptimisticLockingException;
import nl.pancompany.eventstore.annotation.EventHandler;
import nl.pancompany.eventstore.annotation.EventSourced;
import nl.pancompany.eventstore.annotation.StateCreator;
import nl.pancompany.eventstore.data.Event;
import nl.pancompany.eventstore.data.SequencedEvent;
import nl.pancompany.eventstore.exception.StateConstructionFailedException;
import nl.pancompany.eventstore.query.Query;
import nl.pancompany.eventstore.query.Tag;
import nl.pancompany.eventstore.query.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static nl.pancompany.eventstore.data.SequencedEvent.toEvents;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class EventSourcingTest {

    private EventStore eventStore;
    private static final String MY_ENTITY_ID = UUID.randomUUID().toString();
    MyInitialEvent myInitialEvent;
    MyEvent myEvent;
    MyOtherEvent myOtherEvent;
    MyNewEvent myNewEvent;
    MyUnsourcedEvent myUnsourcedEvent;
    Event event0, event1, event2, event3, unsourced;
    Query query;

    @BeforeEach
    void setUp() {
        eventStore = new EventStore();
        myInitialEvent = new MyInitialEvent(MY_ENTITY_ID, "0");
        myEvent = new MyEvent(MY_ENTITY_ID, "1");
        myOtherEvent = new MyOtherEvent(MY_ENTITY_ID, "2");
        myNewEvent = new MyNewEvent(MY_ENTITY_ID, "3");
        myUnsourcedEvent = new MyUnsourcedEvent(MY_ENTITY_ID, "4");
        event0 = Event.of(myInitialEvent, Tag.of("MyEntity", MY_ENTITY_ID));
        event1 = Event.of(myEvent, Tag.of("MyEntity", MY_ENTITY_ID));
        event2 = Event.of(myOtherEvent, Tag.of("MyEntity", MY_ENTITY_ID));
        event3 = Event.of(myNewEvent, Tag.of("MyEntity", MY_ENTITY_ID));
        unsourced = Event.of(myUnsourcedEvent, Tag.of("MyEntity", MY_ENTITY_ID));
        query = Query
                .taggedWith(Tag.of("MyEntity", MY_ENTITY_ID))
                .andHavingType(MyInitialEvent.class, MyEvent.class, MyOtherEvent.class, MyNewEvent.class, MyUnsourcedEvent.class);
    }

    @AfterEach
    void tearDown() {
        eventStore.close();
    }

    @Test
    void rehydratesStateViewModel() {
        // Arrange
        eventStore.append(event0, event1,event2);
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
    void rehydratesParentChildStateViewModel() {
        // Arrange
        eventStore.append(event0, event1,event2);
        List<SequencedEvent> sequencedEvents = eventStore.read(query);
        assertThat(toEvents(sequencedEvents)).containsExactly(event0, event1, event2);

        // Act (just like in a command handler)
        StateManager<MyStateChild> stateManager = eventStore.loadState(MyStateChild.class, query);

        MyStateChild state = stateManager.getState().get();
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
    void stateConstructingEventMustHaveBeenStoredFirst() {
        // Arrange
        eventStore.append(event1, event0, event2);
        List<SequencedEvent> sequencedEvents = eventStore.read(query);
        assertThat(toEvents(sequencedEvents)).containsExactly(event1, event0, event2);

        // Act (just like in a command handler)
        assertThatThrownBy(() -> eventStore.loadState(MyState.class, query)).isInstanceOf(StateConstructionFailedException.class);
    }

    @Test
    void thowsOptimisticLockingException_AndNotAppliesStateChange_WhenQueryResultsGetModifiedDuringStateChange() {
        // Arrange
        eventStore.append(event0, event1, event2);
        List<SequencedEvent> sequencedEvents = eventStore.read(query);
        assertThat(toEvents(sequencedEvents)).containsExactly(event0, event1, event2);

        // Act (just like in a command handler)
        StateManager<MyStateWithoutStateConstructor> stateManager = eventStore.loadState(new MyStateWithoutStateConstructor(), query);

        MyStateWithoutStateConstructor state = stateManager.getState().get();
        // < business rules validation and decision-making (state change or automation) >

        var concurrentlySavedMyEvent = new MyEvent(MY_ENTITY_ID, "5");
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
        // Act (just like in a command handler)
        StateManager<MyState> stateManager = eventStore.loadState(MyState.class, query);

        Optional<MyState> optionalEntity = stateManager.getState();
        // < business rules validation and decision-making (state change or automation) >

        assertThat(optionalEntity).isEmpty();
    }

    @Test
    void applyingRegularEventToUninitializedStateIsNotPossible() {
        // Arrange
        List<SequencedEvent> sequencedEvents = eventStore.read(query);
        assertThat(sequencedEvents).isEmpty();

        // Act (just like in a command handler)
        StateManager<MyState> stateManager = eventStore.loadState(MyState.class, query);


        // < business rules validation and decision-making (state change or automation) >

        assertThatThrownBy(() ->  stateManager.apply(myEvent, Tag.of("MyEntity", MY_ENTITY_ID), Type.of(MyEvent.class)))
                .isInstanceOf(StateConstructionFailedException.class);
    }

    @Test
    void stateClassMustHaveOneNoArgsConstructor() {
        // Arrange
        List<SequencedEvent> sequencedEvents = eventStore.read(query);
        assertThat(sequencedEvents).isEmpty();

        assertThatThrownBy(() ->  eventStore.loadState(InvalidStateClass2.class, query))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void eventSourcedHandlerMustHaveOneEventParameter() {
        // Arrange
        List<SequencedEvent> sequencedEvents = eventStore.read(query);
        assertThat(sequencedEvents).isEmpty();

        assertThatThrownBy(() ->  eventStore.loadState(InvalidStateClass3.class, query))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void applyingCreatorEventToUninitializedStateIsPossible() {
        // Arrange
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

    @Test
    void sourcedEventPublishedInBetweenApplyCallsResultsInOptimisticLockingException() {
        // Arrange
        List<SequencedEvent> sequencedEvents = eventStore.read(query);
        assertThat(sequencedEvents).isEmpty();

        // Act (just like in a command handler)
        StateManager<MyState> stateManager = eventStore.loadState(MyState.class, query);

        // < business rules validation and decision-making (state change or automation) >

        stateManager.apply(myInitialEvent, Tag.of("MyEntity", MY_ENTITY_ID), Type.of(MyInitialEvent.class));
        eventStore.append(event3);
        assertThatThrownBy(() ->  stateManager.apply(myEvent, Tag.of("MyEntity", MY_ENTITY_ID), Type.of(MyEvent.class)))
                .isInstanceOf(StateManagerOptimisticLockingException.class);
    }

    @Test
    void mayAppendAndApplyUnsourcedEvent() {
        // Arrange
        eventStore.append(event0, event1, event2, unsourced);
        List<SequencedEvent> sequencedEvents = eventStore.read(query);
        assertThat(toEvents(sequencedEvents)).containsExactly(event0, event1, event2, unsourced);

        // Act (just like in a command handler)
        StateManager<MyState> stateManager = eventStore.loadState(MyState.class, query);

        MyState state = stateManager.getState().get();
        // < business rules validation and decision-making (state change or automation) >

        stateManager.apply(myUnsourcedEvent, Tag.of("MyEntity", MY_ENTITY_ID), Type.of(MyUnsourcedEvent.class));

        // Assert
        assertThat(state.myHandledEvents).containsExactly(myInitialEvent, myEvent, myOtherEvent);
        sequencedEvents = eventStore.read(query);
        assertThat(toEvents(sequencedEvents)).containsExactly(event0, event1, event2, unsourced, unsourced);
    }

    private static class InvalidStateClass {

        private final List<Object> myHandledEvents = new CopyOnWriteArrayList<>();

        @StateCreator
        private InvalidStateClass(MyInitialEvent event, MyOtherEvent myOtherEvent) {
            myHandledEvents.add(event);
        }

    }

    private static class InvalidStateClass2 {

        InvalidStateClass2(String invalid) {
        }

        @EventSourced
        private void handle(MyEvent event) {
        }

    }

    private static class InvalidStateClass3 {

        @EventSourced
        private void handle(MyEvent event, MyOtherEvent myOtherEvent) {
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

    private static class MyStateChild extends MyStateParent {

        @StateCreator
        private MyStateChild(MyInitialEvent event) {
            myHandledEvents.add(event);
        }

        @EventSourced
        private void handle(MyNewEvent myNewEvent) {
            myHandledEvents.add(myNewEvent);
        }

    }

    private static class MyStateParent {

        final List<Object> myHandledEvents = new CopyOnWriteArrayList<>();

        @EventSourced
        private void handle(MyEvent event) {
            myHandledEvents.add(event);
        }

        @EventSourced
        private void handle(MyOtherEvent myOtherEvent) {
            myHandledEvents.add(myOtherEvent);
        }

        @EventHandler // should not be invoked, subclass method takes precedence
        private void handle(MyNewEvent myNewEvent) {
            throw new IllegalStateException("should not be invoked");
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

    record MyUnsourcedEvent(String id, String data) {
    }
}
