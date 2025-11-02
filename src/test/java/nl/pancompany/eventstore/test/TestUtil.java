package nl.pancompany.eventstore.test;

import nl.pancompany.eventstore.EventBus;

import java.util.logging.Level;
import java.util.logging.Logger;

public class TestUtil {

    private TestUtil() {
    }

    public static void withoutLogging(Runnable runnable) {
        Logger.getLogger(EventBus.class.getName()).setLevel(Level.OFF);
        runnable.run();
        Logger.getLogger(EventBus.class.getName()).setLevel(Level.INFO);
    }
}
