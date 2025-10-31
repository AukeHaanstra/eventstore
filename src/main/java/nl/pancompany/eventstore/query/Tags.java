package nl.pancompany.eventstore.query;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;

public class Tags {

    private final Set<Tag> tags;

    Tags(Set<Tag> tags) {
        this.tags = new HashSet<>(tags);
    }

    public static Tags all() {
        return new Tags(emptySet());
    }

    public static Tags and(String... tags) {
        return new Tags(Arrays.stream(tags).map(Tag::of).collect(Collectors.toSet()));
    }

    public static Tags and(Tag... tags) {
        return new Tags(Set.of(tags));
    }

    public static Tags and(Set<String> tags) {
        return new Tags(tags.stream().map(Tag::of).collect(Collectors.toSet()));
    }

    public Tags andTag(String tag) {
        tags.add(Tag.of(tag));
        return this;
    }

    public Set<Tag> toSet() {
        return new HashSet<>(tags);
    }

    public boolean isAll() {
        return isAll(tags);
    }

    public static boolean isAll(Set<Tag> tags) {
        return tags.isEmpty();
    }

}
