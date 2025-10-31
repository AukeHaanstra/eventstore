package nl.pancompany.eventstore.query;

import java.util.Set;

public record Tag(String tag) {

    public static Tag of(String tag) {
        return new Tag(tag);
    }

    /**
     * Since aggregates and entities can be completely implicit when using DCB,
     * this method provides a way to relate the concept of a tag to an event stream of an entity.
     *
     * @param entityName The name of the business entity this event relates to
     * @param id The id of the business entity this event relates to
     * @return A DCB tag for the event
     */
    public static Tag of(String entityName, String id) {
        return new Tag(entityName + ":" + id);
    }

    public Tags andTag(String tag) {
        return new Tags(Set.of(this, new Tag(tag)));
    }
}
