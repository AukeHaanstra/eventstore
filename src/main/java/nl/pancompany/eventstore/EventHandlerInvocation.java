package nl.pancompany.eventstore;

public record EventHandlerInvocation(InvocableMethod invocableMethod, SequencedEvent event) {

    public boolean isInvokable() {
        return invocableMethod() != null && event != null;
    }
}
