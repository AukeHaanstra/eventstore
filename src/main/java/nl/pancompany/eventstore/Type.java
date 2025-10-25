package nl.pancompany.eventstore;

import java.util.Set;

public record Type(String type) {

    public static Type of(String type) {
        return new Type(type);
    }

    public static Type of(Class<?> clazz) {
        return new Type(getName(clazz));
    }

    private static String getName(Class<?> clazz) {
        if (clazz.getCanonicalName() == null) {
            throw new IllegalArgumentException("Only classes with canonical names allowed for auto event-typing.");
        }
        return clazz.getCanonicalName();
    }

    public Types orType(String type) {
        return new Types(Set.of(this, new Type(type)));
    }

    public Types orType(Class<?> clazz) {
        return new Types(Set.of(this, Type.of(clazz)));
    }
}
