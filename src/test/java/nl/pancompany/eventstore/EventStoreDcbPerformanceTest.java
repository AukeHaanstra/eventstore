package nl.pancompany.eventstore;

import nl.pancompany.eventstore.EventStore.Event;
import nl.pancompany.eventstore.EventStore.SequencedEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Result querying more than a million events with a complex query, on a dev laptop takes approx. just over half a second.
 */
public class EventStoreDcbPerformanceTest {

    private EventStore eventStore;

    @BeforeEach
    void setUp() {
        eventStore = new EventStore();
    }

    @Test
    public void canFilterEventsFromThousands() {
        List<Event> events = new ArrayList<>();
        String[] myEntityIds = { "1", "2", "3" };
        String[] otherEntityIds = { "4", "5", "6" };

        /*
        Total: 1200001 events (100000*6*2 + 1), rotating in a sequence of 12 unique tag + type combinations:

        j      Tags follow the sequence:     Types (abbreviated) follow the sequence:    Queried:
        1      MyEntity1:1                   MyEvent1
        1      MyEntity1:1                   MyEvent2                                    yes (first iteration (j): event2)
        2      MyEntity2:2, OtherEntity2:5   MyEvent1
        2      MyEntity2:2, OtherEntity2:5   MyEvent2
        3      MyEntity3:3, OtherEntity3:6   MyEvent1                                    yes (first iteration (j): event5)
        3      MyEntity3:3, OtherEntity3:6   MyEvent2
        4      MyEntity4:1                   MyEvent1
        4      MyEntity4:1                   MyEvent2
        5      MyEntity5:2, OtherEntity5:5   MyEvent1
        5      MyEntity5:2, OtherEntity5:5   MyEvent2                                    yes (first iteration (j): event10)
        6      MyEntity6:3, OtherEntity6:6   MyEvent1                                    yes (first iteration (j): event11)
        6      MyEntity6:3, OtherEntity6:6   MyEvent2                                    yes (first iteration (j): event12)

        5 events queried => 5*100000 = 500000 events + 1 special event => 500001 events expected
         */

        // iterations1 >= 100, otherwise the special event is skipped
        // 100 -> 8 ms (= 12*100+1 = 1201 events)
        // 1000 -> 20 ms
        // 10000 -> 120 ms
        // 100000 -> 600 ms
        // 1000000 -> OutOfMemoryError: Java heap space
        int iterations1 = 100000;

        int iterations2 = 6;
        int eventNo = 0;
        for (int i = 0; i < iterations1; i++) {
            if (i == 42) {
                events.add(Event.of(new SpecialEvent("event" + (42 * iterations2 * 2) + "*"))); // event504*
            }
            for (int j = 1; j < (iterations2 + 1); j++) {
                if ((j - 1) % 3 == 0) {
                    events.add(Event.of(new MyEvent1("event" + ++eventNo),
                            Type.of("MyEvent1"),
                            "MyEntity" + j + ":" + myEntityIds[(j - 1) % 3]));
                    events.add(Event.of(new MyEvent2("event" + ++eventNo),
                            Type.of("MyEvent2"),
                            "MyEntity" + j + ":" + myEntityIds[(j - 1) % 3]));
                } else {
                    events.add(Event.of(new MyEvent1("event" + ++eventNo),
                            Type.of("MyEvent1"),
                            "MyEntity" + j + ":" + myEntityIds[(j - 1) % 3],
                            "OtherEntity" + j + ":" + otherEntityIds[(j - 1) % 3]));
                    events.add(Event.of(new MyEvent2("event" + ++eventNo),
                            Type.of("MyEvent2"),
                            "MyEntity" + j + ":" + myEntityIds[(j - 1) % 3],
                            "OtherEntity" + j + ":" + otherEntityIds[(j - 1) % 3]));
                }
            }
        }

        events.forEach(eventStore::append);

        long millisStart = System.currentTimeMillis();
        List<SequencedEvent> filteredEvents = eventStore.read(
                Query.or(
                        QueryItem.of("MyEntity1:1", "MyEvent2"),
                        QueryItem.taggedWith("MyEntity3:3", "OtherEntity3:6").andHavingType("MyEvent1"),
                        QueryItem.taggedWith("MyEntity5:2", "OtherEntity5:5").andHavingType("MyEvent2"),
                        QueryItem.taggedWith("MyEntity6:3", "OtherEntity6:6").build(),
                        QueryItem.havingType(SpecialEvent.class).build()
                )
        );
        long millisEnd = System.currentTimeMillis();

        assertThat(filteredEvents).hasSize(5 * iterations1 + 1);
        System.out.printf("Total events in event store: %s.%nEvents queried: %s%n", 2 * iterations1 * iterations2, filteredEvents.size());
        System.out.printf("Querying the event store took %s milliseconds%n", millisEnd - millisStart);

        assertThat(filteredEvents.get(0).payload(MyEvent2.class).data).isEqualTo("event2");
        assertThat(filteredEvents.get(1).payload(MyEvent1.class).data).isEqualTo("event5");
        assertThat(filteredEvents.get(2).payload(MyEvent2.class).data).isEqualTo("event10");
        assertThat(filteredEvents.get(3).payload(MyEvent1.class).data).isEqualTo("event11");
        assertThat(filteredEvents.get(4).payload(MyEvent2.class).data).isEqualTo("event12");
        // 42 * 5 (filtered events) = 210
        assertThat(filteredEvents.get(210).payload(SpecialEvent.class).data).isEqualTo("event504*");
        // for iterations1 = 100000: index: 500001, event1200000
        assertThat(filteredEvents.get(iterations1 * 5).payload(MyEvent2.class).data).isEqualTo("event" + eventNo);
    }

    record MyEvent1(String id, String data) {

        public MyEvent1(String data) {
            this(UUID.randomUUID().toString(), data);
        }
    }

    record MyEvent2(String id, String data) {

        public MyEvent2(String data) {
            this(UUID.randomUUID().toString(), data);
        }
    }

    record SpecialEvent(String id, String data) {

        public SpecialEvent(String data) {
            this(UUID.randomUUID().toString(), data);
        }
    }


}
