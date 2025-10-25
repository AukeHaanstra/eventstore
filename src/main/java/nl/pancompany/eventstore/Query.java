package nl.pancompany.eventstore;

import lombok.Getter;

import java.util.HashSet;
import java.util.Set;

public class Query {

    @Getter
    private final Set<QueryItem> queryItems;

    private Query(Set<QueryItem> queryItems) {
        this.queryItems = queryItems;
    }

    public static Query all() {
        return fromItem(QueryItem.all());
    }

    public static Query fromItem(QueryItem queryItems) {
        return new Query(Set.of(queryItems));
    }

    public static Query fromItems(QueryItems queryItems) {
        return new Query(queryItems.toSet());
    }

    public boolean isAll() {
        return new QueryItems(queryItems).isAll();
    }

    // QueryItems delegate

    public static Query or(QueryItem... items) {
        return fromItems(new QueryItems(Set.of(items)));
    }

    // QueryItem delegates

    public static EitherOrQueryBuilder either(String tag, String type) {
        return new EitherOrQueryBuilder().or(tag, type);
    }

    public static class EitherOrQueryBuilder {

        private final Set<QueryItem> queryItems = new HashSet<>();

        public EitherOrQueryBuilder or(String tag, String type) {
            queryItems.add(QueryItem.of(tag, type));
            return this;
        }

        public Query build() {
            return new Query(this.queryItems);
        }

    }

    public static Query of(String tag, String type) {
        return fromItem(QueryItem.of(tag, type));
    }

    public static Query of(String tag, Class<?> clazz) {
        return fromItem(QueryItem.of(tag, clazz));
    }

    public static Query of(Tag tag, Type type) {
        return fromItem(QueryItem.of(tag, type));
    }

    public static Query of(Tags tags, Types types) {
        return fromItem(QueryItem.of(tags, types));
    }

    public static Query of(Tags tags, Type type) {
        return fromItem(QueryItem.of(tags, type));
    }

    public static Query of(Tag tag, Types types) {
        return fromItem(QueryItem.of(tag, types));
    }

    public static AndHavingType taggedWith(Tag... tags) {
        return new AndHavingType(QueryItem.taggedWith(tags));
    }

    public static AndHavingType taggedWith(String... tags) {
        return new AndHavingType(QueryItem.taggedWith(tags));
    }

    public static AndHavingType taggedWith(Set<String> tags) {
        return new AndHavingType(QueryItem.taggedWith(tags));
    }

    public static AndHavingType taggedWith(Tags tags) {
        return new AndHavingType(QueryItem.taggedWith(tags));
    }

    public static AndTaggedWith havingType(Class<?>... classes) {
        return new AndTaggedWith(QueryItem.havingType(classes));
    }

    public static AndTaggedWith havingType(Type... types) {
        return new AndTaggedWith(QueryItem.havingType(types));
    }

    public static Query.AndTaggedWith havingType(String... types) {
        return new AndTaggedWith(QueryItem.havingType(types));
    }

    public static Query.AndTaggedWith havingType(Set<String> types) {
        return new AndTaggedWith(QueryItem.havingType(types));
    }

    public static Query.AndTaggedWith havingType(Types types) {
        return new AndTaggedWith(QueryItem.havingType(types));
    }

    public record AndTaggedWith(QueryItem.AndTaggedWith andTaggedWith) {
        public Query andTaggedWith(Tag... tags) {
            return fromItem(andTaggedWith.andTaggedWith(tags));
        }

        public Query andTaggedWith(String... tags) {
            return fromItem(andTaggedWith.andTaggedWith(tags));
        }

        public Query andTaggedWith(Set<String> tags) {
            return fromItem(andTaggedWith.andTaggedWith(tags));
        }

        public Query andTaggedWith(Tags tags) {
            return fromItem(andTaggedWith.andTaggedWith(tags));
        }

        public Query build() {
            return fromItem(andTaggedWith.build());
        }
    }

    public record AndHavingType(QueryItem.AndHavingType andHavingType) {

        public Query andHavingType(Class<?>... classes) {
            return fromItem(andHavingType.andHavingType(classes));
        }

        public Query andHavingType(Type... types) {
            return fromItem(andHavingType.andHavingType(types));
        }

        public Query andHavingType(String... types) {
            return fromItem(andHavingType.andHavingType(types));
        }

        public Query andHavingType(Set<String> types) {
            return fromItem(andHavingType.andHavingType(types));
        }

        public Query andHavingType(Types types) {
            return fromItem(andHavingType.andHavingType(types));
        }

        public Query build() {
            return fromItem(andHavingType.build());
        }
    }

}
