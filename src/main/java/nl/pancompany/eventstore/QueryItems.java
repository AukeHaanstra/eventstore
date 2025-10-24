package nl.pancompany.eventstore;

import java.util.ArrayList;
import java.util.List;

public class QueryItems {

    private final List<QueryItem> items;

    QueryItems(List<QueryItem> items) {
        this.items = new ArrayList<>(items);
    }

    public static QueryItems all() {
        return new QueryItems(List.of(QueryItem.all()));
    }

    public static QueryItems or(QueryItem... items) {
        return new QueryItems(List.of(items));
    }

    public QueryItems orItemOf(String tag, String type) {
        items.add(QueryItem.of(tag, type));
        return this;
    }

    List<QueryItem> toList() {
        return new ArrayList<>(items);
    }
}
