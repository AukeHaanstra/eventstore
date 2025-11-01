package nl.pancompany.eventstore.record;

import nl.pancompany.eventstore.query.Tag;
import nl.pancompany.eventstore.query.Type;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public record Event(Object payload, Set<Tag> tags, Type type, Map<String, String> metadata) {

    public Event(Object payload, Set<Tag> tags, Type type) {
        this(payload, tags, type, null);
    }

    public Event(Object payload) {
        requireNonNull(payload);
        this(payload, Collections.emptySet(), getName(payload));
    }

    public Event(Object payload, Type type) {
        requireNonNull(payload);
        requireNonNull(type);
        this(payload, Collections.emptySet(), type);
    }

    public Event(Object payload, Tag... tags) {
        requireNonNull(payload);
        requireNonNull(tags);
        this(payload, Set.of(tags), getName(payload));
    }

    public Event(Object payload, Set<Tag> tags) {
        requireNonNull(payload);
        requireNonNull(tags);
        this(payload, tags, getName(payload));
    }

    public Event(Object payload, String... tags) {
        requireNonNull(payload);
        requireNonNull(tags);
        this(payload, Arrays.stream(tags).map(Tag::of).collect(Collectors.toSet()), getName(payload));
    }

    public Event(Object payload, Type type, Tag... tags) {
        requireNonNull(payload);
        requireNonNull(type);
        requireNonNull(tags);
        this(payload, Set.of(tags), type);
    }

    public Event(Object payload, Type type, Set<Tag> tags) {
        requireNonNull(payload);
        requireNonNull(type);
        requireNonNull(tags);
        this(payload, tags, type);
    }

    public Event(Object payload, Type type, String... tags) {
        requireNonNull(payload);
        requireNonNull(type);
        requireNonNull(tags);
        this(payload, Arrays.stream(tags).map(Tag::of).collect(Collectors.toSet()), type);
    }

    public static Event of(Object payload) {
        return new Event(payload);
    }

    public static Event of(Object payload, Type type) {
        return new Event(payload, type);
    }

    public static Event of(Object payload, Tag... tags) {
        return new Event(payload, tags);
    }

    public static Event of(Object payload, Set<Tag> tags) {
        return new Event(payload, tags);
    }

    public static Event of(Object payload, String... tags) {
        return new Event(payload, tags);
    }

    public static Event of(Object payload, Type type, Tag... tags) {
        return new Event(payload, type, tags);
    }

    public static Event of(Object payload, Type type, Set<Tag> tags) {
        return new Event(payload, tags, type);
    }

    public static Event of(Object payload, Type type, String... tags) {
        return new Event(payload, type, tags);
    }

    public Event withMetadata(Map<String, String> metadata) {
        return new Event(payload, tags, type, metadata);
    }

    private static Type getName(Object payload) {
        return Type.of(payload.getClass());
    }

}
