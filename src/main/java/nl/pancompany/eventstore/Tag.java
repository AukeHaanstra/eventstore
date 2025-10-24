package nl.pancompany.eventstore;

import java.util.Set;

public record Tag(String tag) {

    public static Tag of(String tag) {
        return new Tag(tag);
    }

    public Tags andTag(String tag) {
        return new Tags(Set.of(this, new Tag(tag)));
    }
}
