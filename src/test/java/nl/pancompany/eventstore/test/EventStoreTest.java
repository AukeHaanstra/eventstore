package nl.pancompany.eventstore.test;

import nl.pancompany.eventstore.EventStore;
import nl.pancompany.eventstore.record.Event;
import nl.pancompany.eventstore.record.SequencedEvent;
import nl.pancompany.eventstore.query.Query;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.*;

public class EventStoreTest {

    record MyEvent(String data) {
    }

    private EventStore eventStore;

    @BeforeEach
    void setUp() {
        eventStore = new EventStore();
    }

    @AfterEach
    void tearDown() {
        eventStore.close();
    }

    @Test
    public void storesSingleEvent() {
        var myEvent = new Event(new MyEvent("test"));

        eventStore.append(myEvent);
        List<SequencedEvent> sequencedEvents = eventStore.read(Query.all());

        assertThat(sequencedEvents.size()).isEqualTo(1);
        assertThat(toEvents(sequencedEvents)).contains(myEvent);
        assertThatCode(() -> {
            MyEvent retrievedEvent = sequencedEvents.getFirst().payload(MyEvent.class);
        }).doesNotThrowAnyException();
        assertThatCode(() -> sequencedEvents.getFirst().payload(Object.class)).doesNotThrowAnyException();
        assertThatThrownBy(() -> sequencedEvents.getFirst().payload(String.class)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void nullEventsNotAllowed() {
        assertThatThrownBy(() -> new Event(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> eventStore.append((Event) null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> eventStore.append(singletonList(null))).isInstanceOf(NullPointerException.class);
        eventStore.append(); // allowed
        eventStore.append(emptyList()); // allowed
    }

    @Test
    public void storesMultipleEvents() {
        var myEvent1 = new Event(new MyEvent("test"));
        var myEvent2 = new Event(new MyEvent("test2"));

        eventStore.append(myEvent1);
        eventStore.append(myEvent2);
        List<SequencedEvent> sequencedEvents = eventStore.read(Query.all());

        assertThat(sequencedEvents.size()).isEqualTo(2);
        assertThat(toEvents(sequencedEvents)).contains(myEvent1, myEvent2);
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
                    List<SequencedEvent> sequencedEvents = eventStore.read(Query.all()); // read many messages
                    // Test for ConcurrentModificationException by iterating over the list
                    sequencedEvents.forEach(event -> {});
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
        List<SequencedEvent> sequencedEvents = eventStore.read(Query.all());
        assertThat(sequencedEvents).hasSize(expectedEventsSize); // Check for duplicate writes on the same index (then size smaller than expected)
        assertThat(toEvents(sequencedEvents)).containsExactlyInAnyOrderElementsOf(myEvents);
        assertThat(new java.util.HashSet<>(sequencedEvents)).hasSize(expectedEventsSize);
    }

    private static List<Event> toEvents(List<SequencedEvent> sequencedEvents) {
        return sequencedEvents.stream().map(SequencedEvent::toEvent).toList();
    }

}
