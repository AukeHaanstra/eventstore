package nl.pancompany.eventstore.exception;

public class StateConstructionFailedException extends RuntimeException {

    public StateConstructionFailedException(String message) {
        super(message);
    }

    public StateConstructionFailedException(Throwable cause) {
        super(cause);
    }

}
