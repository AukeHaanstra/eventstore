package nl.pancompany.eventstore.record;

import nl.pancompany.eventstore.query.Tag;
import nl.pancompany.eventstore.query.Type;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

public record Event(Object payload, Set<Tag> tags, Type type) {

    public Event(Object payload) {
        this(payload, Collections.emptySet(), getName(payload));
    }

    public Event(Object payload, Type type) {
        this(payload, Collections.emptySet(), type);
    }

    public Event(Object payload, Tag... tags) {
        this(payload, Set.of(tags), getName(payload));
    }

    public Event(Object payload, Set<Tag> tags) {
        this(payload, tags, getName(payload));
    }

    public Event(Object payload, String... tags) {
        this(payload, Arrays.stream(tags).map(Tag::of).collect(Collectors.toSet()), getName(payload));
    }

    public Event(Object payload, Type type, Tag... tags) {
        this(payload, Set.of(tags), type);
    }

    public Event(Object payload, Type type, Set<Tag> tags) {
        this(payload, tags, type);
    }

    public Event(Object payload, Type type, String... tags) {
        this(payload, Arrays.stream(tags).map(Tag::of).collect(Collectors.toSet()), type);
    }

    public static Event of(Object payload) {
        return new Event(payload, Collections.emptySet(), getName(payload));
    }

    public static Event of(Object payload, Type type) {
        return new Event(payload, Collections.emptySet(), type);
    }

    public static Event of(Object payload, Tag... tags) {
        return new Event(payload, Set.of(tags), getName(payload));
    }

    public static Event of(Object payload, Set<Tag> tags) {
        return new Event(payload, tags, getName(payload));
    }

    public static Event of(Object payload, String... tags) {
        return new Event(payload, Arrays.stream(tags).map(Tag::of).collect(Collectors.toSet()), getName(payload));
    }

    public static Event of(Object payload, Type type, Tag... tags) {
        return new Event(payload, Set.of(tags), type);
    }

    public static Event of(Object payload, Type type, Set<Tag> tags) {
        return new Event(payload, tags, type);
    }

    public static Event of(Object payload, Type type, String... tags) {
        return new Event(payload, Arrays.stream(tags).map(Tag::of).collect(Collectors.toSet()), type);
    }

    private static Type getName(Object payload) {
        return Type.of(payload.getClass());
    }

}
