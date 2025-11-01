package nl.pancompany.eventstore.record;

public record LoggedException(String logMessage, Throwable exception) {
    public static LoggedException of(String logMessage, Throwable exception) {
        return new LoggedException(logMessage, exception);
    }
}
