package nl.pancompany.eventstore;

import java.util.HashSet;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;

public class Types {

    private final Set<Type> types;

    Types(Set<Type> types) {
        this.types = new HashSet<>(types);
    }

    public static Types all() {
        return new Types(emptySet());
    }

    public static Types or(String... types) {
        return new Types(Arrays.stream(types).map(Type::of).collect(Collectors.toSet()));
    }

    public static Types or(Class<?>... classes) {
        return new Types(Arrays.stream(classes).map(Type::of).collect(Collectors.toSet()));
    }

    public static Types or(Set<String> types) {
        return new Types(types.stream().map(Type::of).collect(Collectors.toSet()));
    }

    public Types orType(String type) {
        types.add(Type.of(type));
        return this;
    }

    public Types orType(Class<?> clazz) {
        types.add(Type.of(clazz));
        return this;
    }

    Set<Type> toSet() {
        return new HashSet<>(types);
    }

    public boolean isAll() {
        return isAll(types);
    }

    public static boolean isAll(Set<Type> types) {
        return types.isEmpty();
    }
}
