package nl.pancompany.eventstore;

@FunctionalInterface
interface InvocableEventHandler {
    void invoke(Object event);
}
