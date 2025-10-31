package nl.pancompany.eventstore.test;

import nl.pancompany.eventstore.EventStore;
import nl.pancompany.eventstore.annotation.EventHandler;
import nl.pancompany.eventstore.annotation.ResetHandler;
import nl.pancompany.eventstore.record.Event;
import nl.pancompany.eventstore.query.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.lang.System.currentTimeMillis;
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
    }

    @AfterEach
    void tearDown() {
        eventStore.close();
    }

    @Test
    void throwsExceptionOnMultipleNoArgsConstructors() {
        assertThatThrownBy(() -> eventStore.getEventBus().registerSynchronousEventHandlers(InvalidEventHandlerClass.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void throwsExceptionOnInvalidHandlerMethod() {
        assertThatThrownBy(() -> eventStore.getEventBus().registerSynchronousEventHandlers(InvalidEventHandlerClass.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void throwsExceptionOnInvalidHandlerMethod2() {
        assertThatThrownBy(() -> eventStore.getEventBus().registerSynchronousEventHandlers(InvalidEventHandlerClass2.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void throwsExceptionOnInvalidHandlerMethod3() {
        assertThatThrownBy(() -> eventStore.getEventBus().registerSynchronousEventHandlers(InvalidEventHandlerClass3.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void throwsExceptionOnInvalidHandlerMethod4() {
        assertThatThrownBy(() -> eventStore.getEventBus().registerSynchronousEventHandlers(InvalidEventHandlerClass4.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void throwsExceptionOnInvalidHandlerMethod5() {
        assertThatThrownBy(() -> eventStore.getEventBus().registerSynchronousEventHandlers(InvalidEventHandlerClass5.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void registeredSynchronousHandlerHandlesEvent() {
        MyEvent myEvent = new MyEvent("data");

        eventStore.getEventBus().registerSynchronousEventHandlers(EventHandlerClass.class);
        long start = currentTimeMillis();
        eventStore.append(new Event(myEvent));

        assertThat(EventHandlerClass.myHandledEvent).isEqualTo(myEvent);
        long end = currentTimeMillis();
        System.out.printf("Event handled in %s ms from publication.%n",  (end - start));
    }

    @Test
    void registeredSynchronousHandlerHandlesEvent2() {
        SomeOtherEvent someOtherEvent = new SomeOtherEvent("data");

        eventStore.getEventBus().registerSynchronousEventHandlers(EventHandlerClass.class);
        eventStore.append(new Event(someOtherEvent, Type.of("SomeOtherEvent")));

        assertThat(EventHandlerClass.someOtherHandledEvent).isEqualTo(someOtherEvent);
    }

    @Test
    void eventsWithoutHandlerAreSkipped() {
        MyEvent myEvent = new MyEvent("data");

        eventStore.getEventBus().registerSynchronousEventHandlers(NoHandlerClass.class);
        eventStore.append(new Event(myEvent)); // skipped, no exception
    }

    @Test
    void registeredAsynchronousHandlerHandlesEvent() {
        MyEvent myEvent = new MyEvent("data");

        eventStore.getEventBus().registerAsynchronousEventHandlers(EventHandlerClass.class);
        long start = currentTimeMillis();
        eventStore.append(new Event(myEvent));

        await().untilAsserted(() -> assertThat(EventHandlerClass.myHandledEvent).isEqualTo(myEvent));
        long end = currentTimeMillis();
        System.out.printf("Event handled in %s ms from publication.%n",  (end - start));
    }

    @Test
    void registeredSynchronousHandlerInstanceHandlesEvent() {
        MyEvent myEvent = new MyEvent("data");

        eventStore.getEventBus().registerSynchronousEventHandlers(new EventHandlerClass());
        long start = currentTimeMillis();
        eventStore.append(new Event(myEvent));

        assertThat(EventHandlerClass.myHandledEvent).isEqualTo(myEvent);
        long end = currentTimeMillis();
        System.out.printf("Event handled in %s ms from publication.%n",  (end - start));
    }

    @Test
    void registeredAsynchronousHandlerInstanceHandlesEvent() {
        MyEvent myEvent = new MyEvent("data");

        eventStore.getEventBus().registerAsynchronousEventHandlers(new EventHandlerClass());
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

        eventStore.getEventBus().registerSynchronousEventHandlers(MultiEventHandlerClass.class);
        eventStore.append(new Event(myEvent));
        eventStore.append(new Event(myOtherEvent));

        assertThat(MultiEventHandlerClass.myHandledEvent).isEqualTo(myEvent);
        assertThat(MultiEventHandlerClass.myOtherHandledEvent).isEqualTo(myOtherEvent);
    }

    @Test
    void registeredAsynchronousHandlerHandlesMultipleEvents() {
        MyEvent myEvent = new MyEvent("data");
        MyOtherEvent myOtherEvent = new MyOtherEvent("data");

        eventStore.getEventBus().registerAsynchronousEventHandlers(MultiEventHandlerClass.class);
        eventStore.append(new Event(myEvent));
        eventStore.append(new Event(myOtherEvent));

        await().untilAsserted(() -> {
            assertThat(MultiEventHandlerClass.myHandledEvent).isEqualTo(myEvent);
            assertThat(MultiEventHandlerClass.myOtherHandledEvent).isEqualTo(myOtherEvent);
        });
    }

    @Test
    void registeredAsynchronousHandlerLogsExceptionAsWarning() throws InterruptedException {
        MyEvent myEvent = new MyEvent("data");

        eventStore.getEventBus().registerAsynchronousEventHandlers(ThrowingEventHandlerClass.class);
        eventStore.append(new Event(myEvent));

        Thread.sleep(1000); // wait for log message
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

        eventStore.getEventBus().registerAsynchronousEventHandlers(RecordingEventHandlerClass.class);
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
    void resetStreamsSubsetOfEventsAgain() {
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

        eventStore.getEventBus().registerSynchronousEventHandlers(RecordingEventHandlerClass.class);
        events.forEach(event -> eventStore.append(event));
        assertThat(RecordingEventHandlerClass.myHandledEvents).hasSize(batchSize * 2); // 2 different events
        assertThat(RecordingEventHandlerClass.myHandledEvents).containsExactlyElementsOf(myEvents);

        eventStore.getEventBus().replay(EventStore.SequencePosition.of(42));

        assertThat(RecordingEventHandlerClass.myHandledEvents).hasSize(42);
        assertThat(RecordingEventHandlerClass.myHandledEvents).containsExactlyElementsOf(myEvents.subList(0, 42));
    }

    @Test
    void resetStreamsSubsetOfEventsAgainAsync() {
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

        eventStore.getEventBus().registerAsynchronousEventHandlers(RecordingEventHandlerClass.class);
        events.forEach(event -> eventStore.append(event));
        await().untilAsserted(() -> assertThat(RecordingEventHandlerClass.myHandledEvents).hasSize(batchSize * 2));
        assertThat(RecordingEventHandlerClass.myHandledEvents).containsExactlyElementsOf(myEvents);

        eventStore.getEventBus().replay(EventStore.SequencePosition.of(42));

        await().untilAsserted(() -> assertThat(RecordingEventHandlerClass.myHandledEvents).hasSize(42));
        assertThat(RecordingEventHandlerClass.myHandledEvents).containsExactlyElementsOf(myEvents.subList(0, 42));
    }

    public static class RecordingEventHandlerClass {

        private static final List<Object> myHandledEvents = new CopyOnWriteArrayList<>();

        @ResetHandler
        private void resetHandledEvents() {
            myHandledEvents.clear();
        }

        @EventHandler
        private void handle(MyEvent event) {
            myHandledEvents.add(event);
        }

        @EventHandler
        private void handle(MyOtherEvent myOtherEvent) {
            myHandledEvents.add(myOtherEvent);
        }

    }

    public static class ThrowingEventHandlerClass {

        @EventHandler
        private void handle(MyEvent event) {
            throw new RuntimeException("test");
        }

    }

    private static class NoHandlerClass {

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
