package nl.pancompany.eventstore.query;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public record QueryItem(Set<Tag> tags, Set<Type> types) {

    public boolean isAll() {
        return isAllTags() && isAllTypes();
    }

    public boolean isAllTags() {
        return Tags.isAll(tags);
    }

    public boolean isAllTypes() {
        return Types.isAll(types);
    }

    public static QueryItem all() {
        return QueryItem.of(Tags.all(), Types.all());
    }

    public static QueryItem of(String tag, Class<?> clazz) {
        return new QueryItem(Set.of(Tag.of(tag)), Set.of(Type.of(clazz)));
    }

    public static QueryItem of(String tag, String type) {
        return new QueryItem(Set.of(Tag.of(tag)), Set.of(Type.of(type)));
    }

    public static QueryItem of(Tag tag, Type type) {
        return new QueryItem(Set.of(tag), Set.of(type));
    }

    public static QueryItem of(Tags tags, Types types) {
        return new QueryItem(tags.toSet(), types.toSet());
    }

    public static QueryItem of(Tags tags, Type type) {
        return new QueryItem(tags.toSet(), Set.of(type));
    }

    public static QueryItem of(Tag tag, Types types) {
        return new QueryItem(Set.of(tag), types.toSet());
    }

    public QueryItems orItemOf(String tag, String type) {
        return new QueryItems(Set.of(this, of(tag, type)));
    }

    public QueryItems orItemOf(String tag, Class<?> clazz) {
        return new QueryItems(Set.of(this, of(tag, clazz)));
    }

    public static AndHavingType taggedWith(Tag... tags) {
        return new QueryItem(Set.of(tags), Types.all().toSet()).new AndHavingType();
    }

    public static AndHavingType taggedWith(String... tags) {
        Set<Tag> newTags = Arrays.stream(tags).map(Tag::new).collect(Collectors.toSet());
        return new QueryItem(newTags, Types.all().toSet()).new AndHavingType();
    }

    public static AndHavingType taggedWith(Set<String> tags) {
        Set<Tag> newTags = tags.stream().map(Tag::new).collect(Collectors.toSet());
        return new QueryItem(newTags, Types.all().toSet()).new AndHavingType();
    }

    public static AndHavingType taggedWith(Tags tags) {
        return new QueryItem(tags.toSet(), Types.all().toSet()).new AndHavingType();
    }

    public static AndTaggedWith havingType(Type... types) {
        return new QueryItem(Tags.all().toSet(), Set.of(types)).new AndTaggedWith();
    }

    public static AndTaggedWith havingType(String... types) {
        Set<Type> newTypes = Arrays.stream(types).map(Type::new).collect(Collectors.toSet());
        return new QueryItem(Tags.all().toSet(), newTypes).new AndTaggedWith();
    }

    public static AndTaggedWith havingType(Class<?>... classes) {
        Set<Type> newTypes = Arrays.stream(classes).map(Type::of).collect(Collectors.toSet());
        return new QueryItem(Tags.all().toSet(), newTypes).new AndTaggedWith();
    }

    public static AndTaggedWith havingType(Set<String> types) {
        Set<Type> newTypes = types.stream().map(Type::new).collect(Collectors.toSet());
        return new QueryItem(Tags.all().toSet(), newTypes).new AndTaggedWith();
    }

    public static AndTaggedWith havingType(Types types) {
        return new QueryItem(Tags.all().toSet(), types.toSet()).new AndTaggedWith();
    }

    public class AndTaggedWith {
        public QueryItem andTaggedWith(Tag... tags) {
            return new QueryItem(Set.of(tags), types);
        }

        public QueryItem andTaggedWith(String... tags) {
            Set<Tag> newTags = Arrays.stream(tags).map(Tag::new).collect(Collectors.toSet());
            return new QueryItem(newTags, types);
        }

        public QueryItem andTaggedWith(Set<String> tags) {
            Set<Tag> newTags = tags.stream().map(Tag::new).collect(Collectors.toSet());
            return new QueryItem(newTags, types);
        }

        public QueryItem andTaggedWith(Tags tags) {
            return new QueryItem(tags.toSet(), types);
        }

        public QueryItem build() {
            return new QueryItem(Tags.all().toSet(), types);
        }
    }

    public class AndHavingType {

        public QueryItem andHavingType(Type... types) {
            return new QueryItem(tags, Set.of(types));
        }

        public QueryItem andHavingType(String... types) {
            Set<Type> newTypes = Arrays.stream(types).map(Type::new).collect(Collectors.toSet());
            return new QueryItem(tags, newTypes);
        }

        public QueryItem andHavingType(Set<String> types) {
            Set<Type> newTypes = types.stream().map(Type::new).collect(Collectors.toSet());
            return new QueryItem(tags, newTypes);
        }

        public QueryItem andHavingType(Class<?>... classes) {
            Set<Type> newTypes = Arrays.stream(classes).map(Type::of).collect(Collectors.toSet());
            return new QueryItem(tags, newTypes);
        }

        public QueryItem andHavingType(Types types) {
            return new QueryItem(tags, types.toSet());
        }

        public QueryItem build() {
            return new QueryItem(tags, Types.all().toSet());
        }
    }

}



