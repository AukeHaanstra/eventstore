package nl.pancompany.eventstore.test;

import nl.pancompany.eventstore.EventStore;
import nl.pancompany.eventstore.annotation.EventHandler;
import nl.pancompany.eventstore.annotation.ResetHandler;
import nl.pancompany.eventstore.data.Event;
import nl.pancompany.eventstore.data.LoggedException;
import nl.pancompany.eventstore.data.SequencePosition;
import nl.pancompany.eventstore.query.Tag;
import nl.pancompany.eventstore.query.Tags;
import nl.pancompany.eventstore.query.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.lang.System.currentTimeMillis;
import static nl.pancompany.eventstore.test.TestUtil.withoutLogging;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

public class EventHandlerTest {

    private EventStore eventStore;

    @BeforeEach
    void setUp() {
        eventStore = new EventStore();
        EventHandlerClass.myHandledEvent = null;
        MultiEventHandlerClass.myHandledEvent = null;
        MultiEventHandlerClass.myOtherHandledEvent = null;
        RecordingEventHandlerClass.myHandledEvents.clear();
        FilteringEventHandlersClass.myHandledEvents.clear();
        FilteringEventHandlersClass.someOtherHandledEvents.clear();
    }

    @AfterEach
    void tearDown() {
        eventStore.close();
    }

    @Test
    void throwsExceptionOnMultipleNoArgsConstructors() {
        assertThatThrownBy(() -> eventStore.getEventBus().registerSynchronousEventHandler(InvalidEventHandlerClass.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void throwsExceptionOnInvalidHandlerMethod() {
        assertThatThrownBy(() -> eventStore.getEventBus().registerSynchronousEventHandler(InvalidEventHandlerClass.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void throwsExceptionOnInvalidHandlerMethod2() {
        assertThatThrownBy(() -> eventStore.getEventBus().registerSynchronousEventHandler(InvalidEventHandlerClass2.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void throwsExceptionOnInvalidHandlerMethod3() {
        assertThatThrownBy(() -> eventStore.getEventBus().registerSynchronousEventHandler(InvalidEventHandlerClass3.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void throwsExceptionOnInvalidHandlerMethod4() {
        assertThatThrownBy(() -> eventStore.getEventBus().registerSynchronousEventHandler(InvalidEventHandlerClass4.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void throwsExceptionOnInvalidHandlerMethod5() {
        assertThatThrownBy(() -> eventStore.getEventBus().registerSynchronousEventHandler(InvalidEventHandlerClass5.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void registeredSynchronousHandlerHandlesEvent() {
        MyEvent myEvent = new MyEvent("data");

        eventStore.getEventBus().registerSynchronousEventHandler(EventHandlerClass.class);
        long start = currentTimeMillis();
        eventStore.append(new Event(myEvent));

        assertThat(EventHandlerClass.myHandledEvent).isEqualTo(myEvent);
        long end = currentTimeMillis();
        System.out.printf("Event handled in %s ms from publication.%n",  (end - start));
    }

    @Test
    void registeredSynchronousHandlerHandlesEvent2() {
        SomeOtherEvent someOtherEvent = new SomeOtherEvent("data");

        eventStore.getEventBus().registerSynchronousEventHandler(EventHandlerClass.class);
        eventStore.append(new Event(someOtherEvent, Type.of("SomeOtherEvent")));

        assertThat(EventHandlerClass.someOtherHandledEvent).isEqualTo(someOtherEvent);
    }

    @Test
    void eventsWithoutHandlerAreSkipped() {
        MyEvent myEvent = new MyEvent("data");

        eventStore.getEventBus().registerSynchronousEventHandler(NoHandlerClass.class);
        eventStore.append(new Event(myEvent)); // skipped, no exception
    }

    @Test
    void registeredAsynchronousHandlerHandlesEvent() {
        MyEvent myEvent = new MyEvent("data");

        eventStore.getEventBus().registerAsynchronousEventHandler(EventHandlerClass.class);
        long start = currentTimeMillis();
        eventStore.append(new Event(myEvent));

        await().untilAsserted(() -> assertThat(EventHandlerClass.myHandledEvent).isEqualTo(myEvent));
        long end = currentTimeMillis();
        System.out.printf("Event handled in %s ms from publication.%n",  (end - start));
    }

    @Test
    void registeredAsynchronousParentChildHandlersHandleEvents() {
        MyEvent myEvent = new MyEvent("data");
        MyOtherEvent myOtherEvent = new MyOtherEvent("data");
        MyNewEvent myNewEvent = new MyNewEvent("data");

        MyEventHandlerChild instance = eventStore.getEventBus().registerAsynchronousEventHandler(MyEventHandlerChild.class);
        eventStore.append(new Event(myEvent), new Event(myOtherEvent), new Event(myNewEvent));

        await().untilAsserted(() -> assertThat(instance.myHandledEvents).containsExactly(myEvent, myOtherEvent, myNewEvent));
    }

    @Test
    void callsAsynchronousParentChildResetHandlers() {
        MyEventHandlerChild instance = eventStore.getEventBus().registerAsynchronousEventHandler(MyEventHandlerChild.class);
        eventStore.getEventBus().replay(SequencePosition.of(0));

        await().untilAsserted(() -> assertThat(instance.childResetCalled).isTrue());
        await().untilAsserted(() -> assertThat(instance.parentResetCalled).isTrue());
    }

    @Test
    void registeredSynchronousHandlerInstanceHandlesEvent() {
        MyEvent myEvent = new MyEvent("data");

        eventStore.getEventBus().registerSynchronousEventHandler(new EventHandlerClass());
        long start = currentTimeMillis();
        eventStore.append(new Event(myEvent));

        assertThat(EventHandlerClass.myHandledEvent).isEqualTo(myEvent);
        long end = currentTimeMillis();
        System.out.printf("Event handled in %s ms from publication.%n",  (end - start));
    }

    @Test
    void registeredAsynchronousHandlerInstanceHandlesEvent() {
        MyEvent myEvent = new MyEvent("data");

        eventStore.getEventBus().registerAsynchronousEventHandler(new EventHandlerClass());
        long start = currentTimeMillis();
        eventStore.append(new Event(myEvent));

        await().untilAsserted(() -> assertThat(EventHandlerClass.myHandledEvent).isEqualTo(myEvent));
        long end = currentTimeMillis();
        System.out.printf("Event handled in %s ms from publication.%n",  (end - start));
    }

    @Test
    void registeredSynchronousHandlerHandlesMultipleEvents() {
        MyEvent myEvent = new MyEvent("data");
        MyOtherEvent myOtherEvent = new MyOtherEvent("data");

        eventStore.getEventBus().registerSynchronousEventHandler(MultiEventHandlerClass.class);
        eventStore.append(new Event(myEvent));
        eventStore.append(new Event(myOtherEvent));

        assertThat(MultiEventHandlerClass.myHandledEvent).isEqualTo(myEvent);
        assertThat(MultiEventHandlerClass.myOtherHandledEvent).isEqualTo(myOtherEvent);
    }

    @Test
    void registeredAsynchronousHandlerHandlesMultipleEvents() {
        MyEvent myEvent = new MyEvent("data");
        MyOtherEvent myOtherEvent = new MyOtherEvent("data");

        eventStore.getEventBus().registerAsynchronousEventHandler(MultiEventHandlerClass.class);
        eventStore.append(new Event(myEvent));
        eventStore.append(new Event(myOtherEvent));

        await().untilAsserted(() -> {
            assertThat(MultiEventHandlerClass.myHandledEvent).isEqualTo(myEvent);
            assertThat(MultiEventHandlerClass.myOtherHandledEvent).isEqualTo(myOtherEvent);
        });
    }

    @Test
    void eventBusStoresAndLogsException() {
        withoutLogging(() -> {
            eventStore.getEventBus().registerAsynchronousEventHandler(ThrowingEventHandlerClass.class);
            eventStore.append(new Event(new MyEvent("test")));

            await().untilAsserted(() -> assertThat(eventStore.getEventBus().hasLoggedExceptions()).isTrue());
            List<LoggedException> loggedExceptions = eventStore.getEventBus().getLoggedExceptions();
            assertThat(loggedExceptions).hasSize(1);
            LoggedException loggedException = loggedExceptions.getFirst();
            assertThat(loggedException.exception()).isInstanceOf(IllegalArgumentException.class);
            assertThat(loggedException.exception().getMessage()).isEqualTo("test");
            assertThat(loggedException.logMessage()).isNotEmpty();
        });
    }

    @Test
    void eventBusStoresMax100ExceptionsAndOperatesLIFO() {
        withoutLogging(() -> {
            eventStore.getEventBus().registerAsynchronousEventHandler(ThrowingEventHandlerClass.class);
            for (int i = 0; i <= 142; i++) {
                eventStore.append(new Event(new MyEvent("" + i)));
            }

            await().untilAsserted(() -> assertThat(eventStore.getEventBus().hasLoggedExceptions()).isTrue());
            await().untilAsserted(() -> {
                List<LoggedException> loggedExceptions = eventStore.getEventBus().getLoggedExceptions();
                LoggedException lastLoggedException = loggedExceptions.getLast();
                assertThat(lastLoggedException.exception().getMessage()).isEqualTo("142");
                assertThat(loggedExceptions).hasSize(100);
            });
        });
    }

    @Test
    void registeredAsynchronousHandlerHandlesMultipleEventsInOrder() {
        int batchSize = 20000;
        List<Event> events = new ArrayList<>();
        List<Object> myEvents = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            MyEvent myEvent = new MyEvent(Integer.toString(i));
            events.add(new Event(myEvent));
            myEvents.add(myEvent);
            MyOtherEvent myOtherEvent = new MyOtherEvent(Integer.toString(i));
            events.add(new Event(myOtherEvent));
            myEvents.add(myOtherEvent);
        }

        eventStore.getEventBus().registerAsynchronousEventHandler(RecordingEventHandlerClass.class);
        long start = currentTimeMillis();
        events.forEach(event -> eventStore.append(event));
        System.out.println(RecordingEventHandlerClass.myHandledEvents.size() + " events published in " +
                (currentTimeMillis() - start) + " ms from publication.");

        await().untilAsserted(() -> {
            assertThat(RecordingEventHandlerClass.myHandledEvents).hasSize(batchSize * 2); // 2 different events
        });
        long end = currentTimeMillis();
        assertThat(RecordingEventHandlerClass.myHandledEvents).containsExactlyElementsOf(myEvents);
        System.out.println(RecordingEventHandlerClass.myHandledEvents.size() + " events published and handled in "
                + (end - start) + " ms from publication.");
    }

    @Test
    void replayStreamsSubsetOfEventsAgain() {
        int batchSize = 50;
        List<Event> events = new ArrayList<>();
        List<Object> myEvents = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            MyEvent myEvent = new MyEvent(Integer.toString(i));
            events.add(new Event(myEvent));
            myEvents.add(myEvent);
            MyOtherEvent myOtherEvent = new MyOtherEvent(Integer.toString(i));
            events.add(new Event(myOtherEvent));
            myEvents.add(myOtherEvent);
        }

        eventStore.getEventBus().registerSynchronousEventHandler(RecordingEventHandlerClass.class);
        events.forEach(event -> eventStore.append(event));
        assertThat(RecordingEventHandlerClass.myHandledEvents).hasSize(batchSize * 2); // 2 different events
        assertThat(RecordingEventHandlerClass.myHandledEvents).containsExactlyElementsOf(myEvents);

        eventStore.getEventBus().replay(SequencePosition.of(42));

        assertThat(RecordingEventHandlerClass.myHandledEvents).hasSize(42);
        assertThat(RecordingEventHandlerClass.myHandledEvents).containsExactlyElementsOf(myEvents.subList(0, 42));
    }

    @Test
    void replayStreamsSubsetOfEventsAgainAsync() {
        int batchSize = 50;
        List<Event> events = new ArrayList<>();
        List<Object> myEvents = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            MyEvent myEvent = new MyEvent(Integer.toString(i));
            events.add(new Event(myEvent));
            myEvents.add(myEvent);
            MyOtherEvent myOtherEvent = new MyOtherEvent(Integer.toString(i));
            events.add(new Event(myOtherEvent));
            myEvents.add(myOtherEvent);
        }

        eventStore.getEventBus().registerAsynchronousEventHandler(RecordingEventHandlerClass.class);
        events.forEach(event -> eventStore.append(event));
        await().untilAsserted(() -> assertThat(RecordingEventHandlerClass.myHandledEvents).hasSize(batchSize * 2));
        assertThat(RecordingEventHandlerClass.myHandledEvents).containsExactlyElementsOf(myEvents);

        eventStore.getEventBus().replay(SequencePosition.of(42));

        await().untilAsserted(() -> assertThat(RecordingEventHandlerClass.myHandledEvents).hasSize(42));
        assertThat(RecordingEventHandlerClass.myHandledEvents).containsExactlyElementsOf(myEvents.subList(0, 42));
    }

    @Test
    void replayStreamsAllEventsAgainAsync() {
        int batchSize = 50;
        List<Event> events = new ArrayList<>();
        List<Object> myEvents = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            MyEvent myEvent = new MyEvent(Integer.toString(i));
            events.add(new Event(myEvent));
            myEvents.add(myEvent);
            MyOtherEvent myOtherEvent = new MyOtherEvent(Integer.toString(i));
            events.add(new Event(myOtherEvent));
            myEvents.add(myOtherEvent);
        }

        eventStore.getEventBus().registerAsynchronousEventHandler(RecordingEventHandlerClass.class);
        events.forEach(event -> eventStore.append(event));
        await().untilAsserted(() -> assertThat(RecordingEventHandlerClass.myHandledEvents).hasSize(batchSize * 2));
        assertThat(RecordingEventHandlerClass.myHandledEvents).containsExactlyElementsOf(myEvents);

        eventStore.getEventBus().replay();

        await().untilAsserted(() -> assertThat(RecordingEventHandlerClass.myHandledEvents).hasSize(batchSize * 2));
        assertThat(RecordingEventHandlerClass.myHandledEvents).containsExactlyElementsOf(myEvents);
    }

    @Test
    void replayUpToLastSequencePositionStreamsAllEventsAgainAsync() {
        int batchSize = 50;
        List<Event> events = new ArrayList<>();
        List<Object> myEvents = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            MyEvent myEvent = new MyEvent(Integer.toString(i));
            events.add(new Event(myEvent));
            myEvents.add(myEvent);
            MyOtherEvent myOtherEvent = new MyOtherEvent(Integer.toString(i));
            events.add(new Event(myOtherEvent));
            myEvents.add(myOtherEvent);
        }

        eventStore.getEventBus().registerAsynchronousEventHandler(RecordingEventHandlerClass.class);
        events.forEach(event -> eventStore.append(event));
        await().untilAsserted(() -> assertThat(RecordingEventHandlerClass.myHandledEvents).hasSize(batchSize * 2));
        assertThat(RecordingEventHandlerClass.myHandledEvents).containsExactlyElementsOf(myEvents);

        eventStore.getEventBus().replay(eventStore.getLastSequencePosition().get().incrementAndGet()); // querying eventstore for last sequence position

        await().untilAsserted(() -> assertThat(RecordingEventHandlerClass.myHandledEvents).hasSize(batchSize * 2));
        assertThat(RecordingEventHandlerClass.myHandledEvents).containsExactlyElementsOf(myEvents);
    }

    @Test
    void replayStreamsSubsetOfEventsAgainOnlyToReplayEnabledHandlers() {
        int batchSize = 50;
        List<Event> events = new ArrayList<>();
        List<Object> replayableEvents = new ArrayList<>();
        List<Object> nonReplayableEvents = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            MyEvent myEvent = new MyEvent(Integer.toString(i));
            events.add(new Event(myEvent));
            replayableEvents.add(myEvent);
            MyOtherEvent myOtherEvent = new MyOtherEvent(Integer.toString(i));
            events.add(new Event(myOtherEvent));
            replayableEvents.add(myOtherEvent);

            MyNewEvent myNewEvent = new MyNewEvent(Integer.toString(i));
            nonReplayableEvents.add(myNewEvent);
            events.add(new Event(myNewEvent)); // append to event store, but don't put in expectations
        }

        eventStore.getEventBus().registerSynchronousEventHandler(RecordingEventHandlerClass.class);
        events.forEach(event -> eventStore.append(event));
        assertThat(RecordingEventHandlerClass.myHandledEvents).hasSize(batchSize * 3); // 3 different events
        assertThat(RecordingEventHandlerClass.myHandledEvents)
                .containsSubsequence(replayableEvents)
                .containsSubsequence(nonReplayableEvents);

        eventStore.getEventBus().replay(SequencePosition.of(63));

        assertThat(RecordingEventHandlerClass.myHandledEvents).hasSize(42); // only 2/3 of events should have been replayed
        // Don't expect any MyNewEvent
        assertThat(RecordingEventHandlerClass.myHandledEvents).containsExactlyElementsOf(replayableEvents.subList(0, 42));
    }

    private static class MyEventHandlerChild extends MyEventHandlerParent {

        private boolean childResetCalled = false;

        @EventHandler
        private void handle(MyNewEvent myNewEvent) {
            myHandledEvents.add(myNewEvent);
        }

        @ResetHandler
        private void resetHandledEvents() {
            childResetCalled = true;
        }

    }

    private static class MyEventHandlerParent {

        final List<Object> myHandledEvents = new CopyOnWriteArrayList<>();
        boolean parentResetCalled = false;

        @EventHandler
        private void handle(MyEvent event) {
            myHandledEvents.add(event);
        }

        @EventHandler
        private void handle(MyOtherEvent myOtherEvent) {
            myHandledEvents.add(myOtherEvent);
        }

        @EventHandler // should not be invoked, subclass method takes precedence
        private void handle(MyNewEvent myNewEvent) {
            throw new IllegalStateException("should not be invoked");
        }

        @ResetHandler
        private void resetHandledEvents() {
            parentResetCalled = true;
        }


    }

    public static class RecordingEventHandlerClass {

        private static final List<Object> myHandledEvents = new CopyOnWriteArrayList<>();

        @ResetHandler
        private void resetHandledEvents() {
            myHandledEvents.clear();
        }

        @EventHandler(enableReplay = true)
        private void handle(MyEvent event) {
            myHandledEvents.add(event);
        }

        @EventHandler(enableReplay = true)
        private void handle(MyOtherEvent myOtherEvent) {
            myHandledEvents.add(myOtherEvent);
        }

        @EventHandler
        private void handle(MyNewEvent myNewEvent) {
            myHandledEvents.add(myNewEvent);
        }

    }

    public static class ThrowingEventHandlerClass {

        @EventHandler
        private void handle(MyEvent event) {
            throw new IllegalArgumentException(event.data());
        }

    }

    private static class NoHandlerClass {

    }

    @Test
    void filteringEventHandlerShouldReceiveEventWithMatchingTag() {
        MyEvent myEvent = new MyEvent("data");

        eventStore.getEventBus().registerAsynchronousEventHandler(FilteringEventHandlersClass.class);
        eventStore.append(new Event(myEvent, Tag.of("One")));

        await().untilAsserted(() -> {
            assertThat(FilteringEventHandlersClass.myHandledEvents).hasSize(1);
            assertThat(FilteringEventHandlersClass.myHandledEvents).contains(myEvent);
        });
    }

    @Test
    void filteringEventHandlerShouldNotReceiveEventWithNonMatchingTag() throws InterruptedException {
        MyEvent myEvent = new MyEvent("data");

        eventStore.getEventBus().registerAsynchronousEventHandler(FilteringEventHandlersClass.class);
        eventStore.append(new Event(myEvent, Tag.of("Two")));

        Thread.sleep(500);
        assertThat(FilteringEventHandlersClass.myHandledEvents).isEmpty();
    }

    @Test
    void filteringEventHandlerShouldNotReceiveEventWithLessTagsThanRequired() throws InterruptedException {
        SomeOtherEvent someOtherEvent = new SomeOtherEvent("data");

        eventStore.getEventBus().registerAsynchronousEventHandler(FilteringEventHandlersClass.class);
        eventStore.append(new Event(someOtherEvent, Tag.of("Two")));

        Thread.sleep(500);
        assertThat(FilteringEventHandlersClass.someOtherHandledEvents).isEmpty();
    }

    @Test
    void filteringEventHandlerShouldReceiveEventWithMoreTagsThanRequired() {
        SomeOtherEvent someOtherEvent = new SomeOtherEvent("data");

        eventStore.getEventBus().registerAsynchronousEventHandler(FilteringEventHandlersClass.class);
        eventStore.append(Event.of(someOtherEvent, Tags.and("One", "Two", "Three")));

        await().untilAsserted(() -> {
            assertThat(FilteringEventHandlersClass.someOtherHandledEvents).hasSize(1);
            assertThat(FilteringEventHandlersClass.someOtherHandledEvents).contains(someOtherEvent);
        });
    }

    private static class FilteringEventHandlersClass {

        private static List<MyEvent> myHandledEvents = new CopyOnWriteArrayList<>();
        private static List<SomeOtherEvent> someOtherHandledEvents = new CopyOnWriteArrayList<>();

        @EventHandler(requiredTags = "One")
        private void handle(MyEvent event) {
            myHandledEvents.add(event);
        }

        @EventHandler(requiredTags = {"One", "Two"})
        private void handle(SomeOtherEvent event) {
            someOtherHandledEvents.add(event);
        }

    }

    private static class EventHandlerClass {

        private static MyEvent myHandledEvent;
        private static SomeOtherEvent someOtherHandledEvent;

        @EventHandler
        private void handle(MyEvent event) {
            myHandledEvent = event;
        }

        @EventHandler
        private void handle(SomeOtherEvent event) {
        }

        @EventHandler(type = "SomeOtherEvent")
        private void handle(Object event) {
            someOtherHandledEvent = (SomeOtherEvent) event;
        }

        private void someUnannotatedMethod(Object object) {
        }

    }

    public static class MultiEventHandlerClass {

        private static MyEvent myHandledEvent;
        private static MyOtherEvent myOtherHandledEvent;

        @EventHandler
        private void handle(MyEvent myEvent) {
            myHandledEvent = myEvent;
        }

        @EventHandler
        private void handle(MyOtherEvent myOtherEvent) {
            myOtherHandledEvent = myOtherEvent;
        }

    }

    public static class InvalidEventHandlerClass {

        @EventHandler
        private void handle(Object event) {
        }

    }

    public static class InvalidEventHandlerClass2 {

        @EventHandler(type = "Invalid")
        private void handle(MyEvent event) {
        }

    }

    public static class InvalidEventHandlerClass3 {

        @ResetHandler
        private void reset1() {
        }

        @ResetHandler
        private void reset2() {
        }

    }

    public static class InvalidEventHandlerClass4 {

        InvalidEventHandlerClass4(String invalid) {

        }

        @EventHandler
        private void handle(MyEvent myEvent) {
        }

    }

    public static class InvalidEventHandlerClass5 {

        @EventHandler
        private void handle(MyEvent event, MyOtherEvent myOtherEvent) {
        }

    }

    record MyEvent(String id, String data) {

        public MyEvent(String data) {
            this(UUID.randomUUID().toString(), data);
        }
    }

    record MyNewEvent(String id, String data) {

        public MyNewEvent(String data) {
            this(UUID.randomUUID().toString(), data);
        }
    }

    record MyOtherEvent(String id, String data) {

        public MyOtherEvent(String data) {
            this(UUID.randomUUID().toString(), data);
        }
    }

    record SomeOtherEvent(String id, String data) {

        public SomeOtherEvent(String data) {
            this(UUID.randomUUID().toString(), data);
        }
    }
}
