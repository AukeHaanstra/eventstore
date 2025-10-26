package nl.pancompany.eventstore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
        eventStore.append(new EventStore.Event(myEvent));

        assertThat(EventHandlerClass.myHandledEvent).isEqualTo(myEvent);
        long end = currentTimeMillis();
        System.out.printf("Event handled in %s ms from publication.%n",  (end - start));
    }

    @Test
    void registeredAsynchronousHandlerHandlesEvent() {
        MyEvent myEvent = new MyEvent("data");

        eventStore.registerAsynchronousEventHandler(EventHandlerClass.class);
        long start = currentTimeMillis();
        eventStore.append(new EventStore.Event(myEvent));

        await().untilAsserted(() -> assertThat(EventHandlerClass.myHandledEvent).isEqualTo(myEvent));
        long end = currentTimeMillis();
        System.out.printf("Event handled in %s ms from publication.%n",  (end - start));
    }

    @Test
    void registeredSynchronousHandlerHandlesMultipleEvents() {
        MyEvent myEvent = new MyEvent("data");
        MyOtherEvent myOtherEvent = new MyOtherEvent("data");

        eventStore.registerSynchronousEventHandler(MultiEventHandlerClass.class);
        eventStore.append(new EventStore.Event(myEvent));
        eventStore.append(new EventStore.Event(myOtherEvent));

        assertThat(MultiEventHandlerClass.myHandledEvent).isEqualTo(myEvent);
        assertThat(MultiEventHandlerClass.myOtherHandledEvent).isEqualTo(myOtherEvent);
    }

    @Test
    void registeredAsynchronousHandlerHandlesMultipleEvents() {
        MyEvent myEvent = new MyEvent("data");
        MyOtherEvent myOtherEvent = new MyOtherEvent("data");

        eventStore.registerAsynchronousEventHandler(MultiEventHandlerClass.class);
        eventStore.append(new EventStore.Event(myEvent));
        eventStore.append(new EventStore.Event(myOtherEvent));

        await().untilAsserted(() -> {
            assertThat(MultiEventHandlerClass.myHandledEvent).isEqualTo(myEvent);
            assertThat(MultiEventHandlerClass.myOtherHandledEvent).isEqualTo(myOtherEvent);
        });
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
