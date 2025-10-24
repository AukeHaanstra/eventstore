package nl.pancompany.eventstore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EventStoreDcbTest {

    record MyEvent(String payload) {
    }

    EventStore eventStore;

    @BeforeEach
    void setUp() {
        eventStore = new EventStore();
    }

    @Test
    void filtersEvents() {
        // TODO: implement storing tags and types
    }

}


