package nl.pancompany.eventstore;

import java.util.Arrays;
import java.util.List;

public class QueryItem {

    private final List<Tag> tags;
    private final List<Type> types;

    private QueryItem(List<Tag> tags, List<Type> types) {
        this.tags = tags;
        this.types = types;
    }

    public static QueryItem all() {
        return QueryItem.of(Tags.all(), Types.all());
    }

    public static QueryItem of(String tag, String type) {
        return new QueryItem(List.of(Tag.of(tag)), List.of(Type.of(type)));
    }

    public static QueryItem of(Tag tag, Type type) {
        return new QueryItem(List.of(tag), List.of(type));
    }

    public static QueryItem of(Tags tags, Types types) {
        return new QueryItem(tags.toList(), types.toList());
    }

    public static QueryItem of(Tags tags, Type type) {
        return new QueryItem(tags.toList(), List.of(type));
    }

    public static QueryItem of(Tag tag, Types types) {
        return new QueryItem(List.of(tag), types.toList());
    }

    public QueryItems orItemOf(String tag, String type) {
        return new QueryItems(List.of(this, of(tag, type)));
    }

    public static AndHavingType taggedWith(Tag... tags) {
        return new QueryItem(List.of(tags), Types.all().toList()).new AndHavingType();
    }

    public static AndHavingType taggedWith(String... tags) {
        List<Tag> newTags = Arrays.stream(tags).map(Tag::new).toList();
        return new QueryItem(newTags, Types.all().toList()).new AndHavingType();
    }

    public static AndHavingType taggedWith(List<String> tags) {
        List<Tag> newTags = tags.stream().map(Tag::new).toList();
        return new QueryItem(newTags, Types.all().toList()).new AndHavingType();
    }

    public static AndHavingType taggedWith(Tags tags) {
        return new QueryItem(tags.toList(), Types.all().toList()).new AndHavingType();
    }

    public static AndTaggedWith havingType(Type... types) {
        return new QueryItem(Tags.all().toList(), List.of(types)).new AndTaggedWith();
    }

    public static AndTaggedWith havingType(String... types) {
        List<Type> newTypes = Arrays.stream(types).map(Type::new).toList();
        return new QueryItem(Tags.all().toList(), newTypes).new AndTaggedWith();
    }

    public static AndTaggedWith havingType(List<String> types) {
        List<Type> newTypes = types.stream().map(Type::new).toList();
        return new QueryItem(Tags.all().toList(), newTypes).new AndTaggedWith();
    }

    public static AndTaggedWith havingType(Types types) {
        return new QueryItem(Tags.all().toList(), types.toList()).new AndTaggedWith();
    }

    public class AndTaggedWith{
        public QueryItem andTaggedWith(Tag... tags) {
            return new QueryItem(List.of(tags), types);
        }

        public QueryItem andTaggedWith(String... tags) {
            List<Tag> newTags = Arrays.stream(tags).map(Tag::new).toList();
            return new QueryItem(newTags, types);
        }

        public QueryItem andTaggedWith(List<String> tags) {
            List<Tag> newTags = tags.stream().map(Tag::new).toList();
            return new QueryItem(newTags, types);
        }

        public QueryItem andTaggedWith(Tags tags) {
            return new QueryItem(tags.toList(), types);
        }
    }

    public class AndHavingType {
        public QueryItem andHavingType(Type... types) {
            return new QueryItem(tags, List.of(types));
        }

        public QueryItem andHavingType(String... types) {
            List<Type> newTypes = Arrays.stream(types).map(Type::new).toList();
            return new QueryItem(tags, newTypes);
        }

        public QueryItem andHavingType(List<String> types) {
            List<Type> newTypes = types.stream().map(Type::new).toList();
            return new QueryItem(tags, newTypes);
        }

        public QueryItem andHavingType(Types types) {
            return new QueryItem(tags, types.toList());
        }
    }

}



