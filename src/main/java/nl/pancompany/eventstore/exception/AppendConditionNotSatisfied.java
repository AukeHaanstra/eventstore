package nl.pancompany.eventstore.exception;

public class AppendConditionNotSatisfied extends Exception {

    public AppendConditionNotSatisfied(String message) {
        super(message);
    }
}
