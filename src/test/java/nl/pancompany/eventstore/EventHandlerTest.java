package nl.pancompany.eventstore;

import nl.pancompany.eventstore.EventStore.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

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
    }

    @Test
    void throwsExceptionOnInvalidHandlerMethod() {
        assertThatThrownBy(() -> eventStore.registerSynchronousEventHandler(InvalidEventHandlerClass.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void throwsExceptionOnInvalidHandlerMethod2() {
        assertThatThrownBy(() -> eventStore.registerSynchronousEventHandler(InvalidEventHandlerClass2.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void registeredSynchronousHandlerHandlesEvent() {
        MyEvent myEvent = new MyEvent("data");

        eventStore.registerSynchronousEventHandler(EventHandlerClass.class);
        long start = currentTimeMillis();
        eventStore.append(new Event(myEvent));

        assertThat(EventHandlerClass.myHandledEvent).isEqualTo(myEvent);
        long end = currentTimeMillis();
        System.out.printf("Event handled in %s ms from publication.%n",  (end - start));
    }

    @Test
    void registeredAsynchronousHandlerHandlesEvent() {
        MyEvent myEvent = new MyEvent("data");

        eventStore.registerAsynchronousEventHandler(EventHandlerClass.class);
        long start = currentTimeMillis();
        eventStore.append(new Event(myEvent));

        await().untilAsserted(() -> assertThat(EventHandlerClass.myHandledEvent).isEqualTo(myEvent));
        long end = currentTimeMillis();
        System.out.printf("Event handled in %s ms from publication.%n",  (end - start));
    }

    @Test
    void registeredSynchronousHandlerInstanceHandlesEvent() {
        MyEvent myEvent = new MyEvent("data");

        eventStore.registerSynchronousEventHandler(new EventHandlerClass());
        long start = currentTimeMillis();
        eventStore.append(new Event(myEvent));

        assertThat(EventHandlerClass.myHandledEvent).isEqualTo(myEvent);
        long end = currentTimeMillis();
        System.out.printf("Event handled in %s ms from publication.%n",  (end - start));
    }

    @Test
    void registeredAsynchronousHandlerInstanceHandlesEvent() {
        MyEvent myEvent = new MyEvent("data");

        eventStore.registerAsynchronousEventHandler(new EventHandlerClass());
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

        eventStore.registerSynchronousEventHandler(MultiEventHandlerClass.class);
        eventStore.append(new Event(myEvent));
        eventStore.append(new Event(myOtherEvent));

        assertThat(MultiEventHandlerClass.myHandledEvent).isEqualTo(myEvent);
        assertThat(MultiEventHandlerClass.myOtherHandledEvent).isEqualTo(myOtherEvent);
    }

    @Test
    void registeredAsynchronousHandlerHandlesMultipleEvents() {
        MyEvent myEvent = new MyEvent("data");
        MyOtherEvent myOtherEvent = new MyOtherEvent("data");

        eventStore.registerAsynchronousEventHandler(MultiEventHandlerClass.class);
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

        eventStore.registerAsynchronousEventHandler(ThrowingEventHandlerClass.class);
        eventStore.append(new Event(myEvent));

        Thread.sleep(1000); // wait for log message
    }

    @Test
    void registeredAsynchronousHandlerHandlesMultipleEventsInOrder() {
        int batchSize = 10000;
        List<Event> events = new ArrayList<>();
        List<MyEvent> myEvents = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            MyEvent myEvent = new MyEvent(Integer.toString(i));
            events.add(new Event(myEvent));
            myEvents.add(myEvent);
        }

        eventStore.registerAsynchronousEventHandler(RecordingEventHandlerClass.class);
        long start = currentTimeMillis();
        events.forEach(event -> eventStore.append(event));

        await().untilAsserted(() -> {
            assertThat(RecordingEventHandlerClass.myHandledEvents).hasSize(batchSize);
        });
        long end = currentTimeMillis();
        assertThat(RecordingEventHandlerClass.myHandledEvents).containsExactlyElementsOf(myEvents);
        System.out.println(batchSize + " events handled in " + (end - start) + " ms from publication.");
    }

    public static class RecordingEventHandlerClass {

        private static List<MyEvent> myHandledEvents = new ArrayList<>();

        @EventHandler
        private void handle(MyEvent event) {
            myHandledEvents.add(event);
        }

    }

    public static class ThrowingEventHandlerClass {

        @EventHandler
        private void handle(MyEvent event) {
            throw new RuntimeException("test");
        }

    }

    public static class EventHandlerClass {

        private static MyEvent myHandledEvent;

        @EventHandler
        private void handle(MyEvent event) {
            myHandledEvent = event;
        }

        @EventHandler
        private void handle(SomeOtherEvent event) {
        }

        @EventHandler(type = "SomeOtherEvent")
        private void handle(Object event) {
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
