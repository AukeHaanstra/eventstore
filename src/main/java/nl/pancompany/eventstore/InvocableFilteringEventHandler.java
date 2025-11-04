package nl.pancompany.eventstore;

import nl.pancompany.eventstore.query.Tag;

import java.util.Set;

@FunctionalInterface
interface InvocableFilteringEventHandler {
    void invoke(Object event, Set<Tag> eventTagsToMatch);
}
