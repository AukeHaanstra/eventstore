package nl.pancompany.eventstore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;

public class Tags {

    private final List<Tag> tags;

    Tags(List<Tag> tags) {
        this.tags = new ArrayList<>(tags);
    }

    public static Tags all() {
        return new Tags(emptyList());
    }

    public static Tags and(String... tags) {
        return new Tags(Arrays.stream(tags).map(Tag::of).toList());
    }

    public static Tags and(List<String> tags) {
        return new Tags(tags.stream().map(Tag::of).toList());
    }

    public Tags andTag(String tag) {
        tags.add(Tag.of(tag));
        return this;
    }

    List<Tag> toList() {
        return new ArrayList<>(tags);
    }

    public boolean isAll() {
        return tags.isEmpty();
    }
}
