package nl.pancompany.eventstore.query;

import java.util.HashSet;
import java.util.Set;

public class QueryItems {

    private final Set<QueryItem> items;

    QueryItems(Set<QueryItem> items) {
        this.items = new HashSet<>(items);
    }

    public static QueryItems all() {
        return new QueryItems(Set.of(QueryItem.all()));
    }

    public static QueryItems or(QueryItem... items) {
        return new QueryItems(Set.of(items));
    }

    public QueryItems orItemOf(String tag, String type) {
        items.add(QueryItem.of(tag, type));
        return this;
    }

    public QueryItems orItemOf(String tag, Class<?> clazz) {
        items.add(QueryItem.of(tag, clazz));
        return this;
    }

    Set<QueryItem> toSet() {
        return new HashSet<>(items);
    }

    public boolean isAll() {
        return items.stream().anyMatch(QueryItem::isAll);
    }
}
