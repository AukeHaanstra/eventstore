package nl.pancompany.eventstore;

import lombok.Getter;

import java.util.List;

public class Query {

    @Getter
    private final List<QueryItem> queryItems;

    private Query(List<QueryItem> queryItems) {
        this.queryItems = queryItems;
    }

    public static Query all() {
        return fromItem(QueryItem.all());
    }

    public static Query of(String tag, String type) {
        return new Query(List.of(QueryItem.of(Tag.of(tag), Type.of(type))));
    }

    public static Query of(Tag tag, Type type) {
        return new Query(List.of(QueryItem.of(tag, type)));
    }

    public static Query of(Tags tags, Types types) {
        return new Query(List.of(QueryItem.of(tags, types)));
    }

    public static Query fromItem(QueryItem queryItems) {
        return new Query(List.of(queryItems));
    }

    public static Query fromItems(QueryItems queryItems) {
        return new Query(queryItems.toList());
    }

}
