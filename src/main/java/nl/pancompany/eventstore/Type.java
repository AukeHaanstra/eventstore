package nl.pancompany.eventstore;

import java.util.List;

public record Type(String type) {

    public static Type of(String type) {
        return new Type(type);
    }

    public Types orType(String type) {
        return new Types(List.of(this, new Type(type)));
    }
}
