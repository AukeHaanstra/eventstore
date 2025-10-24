package nl.pancompany.eventstore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;

public class Types {

    private final List<Type> types;

    Types(List<Type> types) {
        this.types = new ArrayList<>(types);
    }

    public static Types all() {
        return new Types(emptyList());
    }

    public static Types or(String... types) {
        return new Types(Arrays.stream(types).map(Type::of).toList());
    }

    public static Types or(List<String> types) {
        return new Types(types.stream().map(Type::of).toList());
    }

    public Types orType(String type) {
        types.add(Type.of(type));
        return this;
    }

    List<Type> toList() {
        return new ArrayList<>(types);
    }

    public boolean isAll() {
        return types.isEmpty();
    }
}
