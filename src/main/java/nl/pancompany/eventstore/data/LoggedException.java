package nl.pancompany.eventstore.data;

public record LoggedException(String logMessage, Throwable exception) {
    public static LoggedException of(String logMessage, Throwable exception) {
        return new LoggedException(logMessage, exception);
    }
}
