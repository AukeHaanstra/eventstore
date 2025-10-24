package nl.pancompany.eventstore;

import nl.pancompany.eventstore.EventStore.Event;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;

public class EventStoreTest {

    record MyEvent(String payload) {
    }

    EventStore eventStore;

    @BeforeEach
    void setUp() {
        eventStore = new EventStore();
    }

    @Test
    public void storesSingleEvent() {
        var myEvent = new Event(new MyEvent("test"));

        eventStore.append(myEvent);
        List<Event> events = eventStore.read(Query.all());

        assertThat(events.size()).isEqualTo(1);
        assertThat(events).contains(myEvent);
        assertThatCode(() -> {
            MyEvent retrievedEvent = events.getFirst().payload(MyEvent.class);
        }).doesNotThrowAnyException();
        assertThatCode(() -> events.getFirst().payload(Object.class)).doesNotThrowAnyException();
        assertThatThrownBy(() -> events.getFirst().payload(String.class)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void storesMultipleEvents() {
        var myEvent1 = new Event(new MyEvent("test"));
        var myEvent2 = new Event(new MyEvent("test2"));

        eventStore.append(myEvent1);
        eventStore.append(myEvent2);
        List<Event> events = eventStore.read(Query.all());

        assertThat(events.size()).isEqualTo(2);
        assertThat(events).contains(myEvent1, myEvent2);
    }

    @Test
    public void isThreadSafe() {
        Queue<Event> myEvents = new ConcurrentLinkedQueue<>();

        int writers = 200;
        int taskSize = writers * 5;
        int eventsPerWriter = 100;

        CountDownLatch startGate = new CountDownLatch(taskSize); // increase concurrency

        List<Callable<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < taskSize; i++) {
            final int taskNo = i;
            if (i % (taskSize / writers) == 0) {
                tasks.add(() -> {
                    startGate.countDown();
                    startGate.await();
                    for (int j = 0; j < eventsPerWriter; j++) { // write many messages
                        MyEvent payload = new MyEvent("writer " + taskNo + " event " + j);
                        Event newEvent = new Event(payload);
                        myEvents.add(newEvent);
                        eventStore.append(newEvent);
                    }
                    return null;
                });
            } else {
                tasks.add(() -> {
                    startGate.countDown();
                    startGate.await();
                    List<Event> events = eventStore.read(Query.all()); // read many messages
                    // Test for ConcurrentModificationException by iterating over the list
                    events.forEach(event -> {});
                    return null;
                });
            }
        }

        try (ExecutorService service = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Void>> result = service.invokeAll(tasks);
            result.forEach(future -> {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (InterruptedException e) {
            Assertions.fail(e);
        }

        int expectedEventsSize = writers * eventsPerWriter;
        List<Event> events = eventStore.read(Query.all());
        assertThat(events).hasSize(expectedEventsSize); // Check for duplicate writes on the same index (then size smaller than expected)
        assertThat(events).containsExactlyInAnyOrderElementsOf(myEvents);
        assertThat(new java.util.HashSet<>(events)).hasSize(expectedEventsSize);
    }

}
