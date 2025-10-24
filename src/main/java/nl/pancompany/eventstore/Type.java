package nl.pancompany.eventstore;

import java.util.Set;

public record Type(String type) {

    public static Type of(String type) {
        return new Type(type);
    }

    public Types orType(String type) {
        return new Types(Set.of(this, new Type(type)));
    }
}
