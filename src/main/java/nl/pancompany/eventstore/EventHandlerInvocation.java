package nl.pancompany.eventstore;

import nl.pancompany.eventstore.EventStore.SequencedEvent;

public record EventHandlerInvocation(InvocableMethod invocableMethod, SequencedEvent event) {

    public boolean isInvokable() {
        return invocableMethod() != null && event != null;
    }
}
