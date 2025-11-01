package nl.pancompany.eventstore.record;

public record LoggedException(String logMessage, Exception exception) {
    public static LoggedException of(String logMessage, Exception exception) {
        return new LoggedException(logMessage, exception);
    }
}
