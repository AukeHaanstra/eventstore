package nl.pancompany.eventstore;

import java.util.List;

public record Tag(String tag) {

    public static Tag of(String tag) {
        return new Tag(tag);
    }

    public Tags andTag(String tag) {
        return new Tags(List.of(this, new Tag(tag)));
    }
}
