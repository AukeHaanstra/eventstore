package nl.pancompany.eventstore.api;

import nl.pancompany.eventstore.*;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static nl.pancompany.eventstore.Query.all;

public class QueryApiTest {

    @Test
    public void createQueryTest() {
        Query query;
        // synonyms - all
        query = all();
        query = Query.all();
        query = Query.fromItem(QueryItem.all());
        query = Query.fromItems(QueryItems.all());
        query = Query.of(Tags.all(), Types.all());
        query = Query.fromItem(
                QueryItem.of(Tags.all(), Types.all())
        );
        query = Query.taggedWith(Tags.all()).andHavingType(Types.all());
        query = Query.fromItem(
                QueryItem.taggedWith(Tags.all()).andHavingType(Types.all())
        );
        query = Query.havingType(Types.all()).andTaggedWith(Tags.all());
        query = Query.fromItem(
                QueryItem.havingType(Types.all()).andTaggedWith(Tags.all())
        );

        // synonyms - one item with one tag and one type
        query = Query.of("myTag","myType");
        query = Query.of(Tag.of("myTag"), Type.of("myType"));
        query = Query.of("myTag","myType");
        query = Query.fromItem(
                QueryItem.of("myTag","myType")
        );
        query = Query.of(Tag.of("myTag"), Type.of("myType"));
        query = Query.fromItem(
                QueryItem.of(Tag.of("myTag"), Type.of("myType"))
        );

        Set<String> tagStrings = Set.of("myTag", "otherTag");
        Set<String> typeStrings = Set.of("myType", "otherType");
        // synonyms - one item with multiple tags and multiple types
        query = Query.of(Tags.and("myTag", "otherTag"), Types.or("myType", "otherType"));
        query = Query.fromItem(
                QueryItem.of(Tags.and("myTag", "otherTag"), Types.or("myType", "otherType"))
        );
        query = Query.of(Tags.and(tagStrings), Types.or(typeStrings));
        query = Query.fromItem(
                QueryItem.of(Tags.and(tagStrings), Types.or(typeStrings))
        );
        query = Query.of(Tag.of("myTag").andTag("otherTag").andTag("yetAnotherTag"),
                Type.of("myType").orType("otherType").orType("yetAnotherType"));
        query = Query.fromItem(
                QueryItem.of(Tag.of("myTag").andTag("otherTag").andTag("yetAnotherTag"),
                        Type.of("myType").orType("otherType").orType("yetAnotherType"))
        );
        query = Query.taggedWith("myTag", "otherTag").andHavingType("myType", "otherType");
        query = Query.fromItem(
                QueryItem.taggedWith("myTag", "otherTag").andHavingType("myType", "otherType")
        );
        query = Query.taggedWith(tagStrings).andHavingType(typeStrings);
        query = Query.fromItem(
                QueryItem.taggedWith(tagStrings).andHavingType(typeStrings)
        );
        query = Query.taggedWith(Tag.of("myTag").andTag("otherTag").andTag("yetAnotherTag"))
                .andHavingType(Type.of("myType").orType("otherType").orType("yetAnotherType"));
        query = Query.fromItem(
                QueryItem.taggedWith(Tag.of("myTag").andTag("otherTag").andTag("yetAnotherTag"))
                        .andHavingType(Type.of("myType").orType("otherType").orType("yetAnotherType"))
        );
        query = Query.taggedWith(Tag.of("myTag"), Tag.of("otherTag"), Tag.of("yetAnotherTag"))
                .andHavingType(Type.of("myType"), Type.of("otherType"), Type.of("yetAnotherType"));
        query = Query.fromItem(
                QueryItem.taggedWith(Tag.of("myTag"), Tag.of("otherTag"), Tag.of("yetAnotherTag"))
                        .andHavingType(Type.of("myType"), Type.of("otherType"), Type.of("yetAnotherType"))
        );
        query = Query.havingType("myType", "otherType").andTaggedWith("myTag", "otherTag");
        query = Query.fromItem(
                QueryItem.havingType("myType", "otherType").andTaggedWith("myTag", "otherTag")
        );
        query = Query.havingType(typeStrings).andTaggedWith(tagStrings);
        query = Query.fromItem(
                QueryItem.havingType(typeStrings).andTaggedWith(tagStrings)
        );
        query = Query.havingType(Type.of("myType").orType("otherType").orType("yetAnotherType"))
                .andTaggedWith(Tag.of("myTag").andTag("otherTag").andTag("yetAnotherTag"));
        query = Query.fromItem(
                QueryItem.havingType(Type.of("myType").orType("otherType").orType("yetAnotherType"))
                        .andTaggedWith(Tag.of("myTag").andTag("otherTag").andTag("yetAnotherTag"))

        );
        query = Query.havingType(Type.of("myType"), Type.of("otherType"), Type.of("yetAnotherType"))
                .andTaggedWith(Tag.of("myTag"), Tag.of("otherTag"), Tag.of("yetAnotherTag"));
        query = Query.fromItem(
                QueryItem.havingType(Type.of("myType"), Type.of("otherType"), Type.of("yetAnotherType"))
                        .andTaggedWith(Tag.of("myTag"), Tag.of("otherTag"), Tag.of("yetAnotherTag"))

        );
        // variations - mixing with single tag or single type
        query = Query.of(Tag.of("myTag").andTag("otherTag").andTag("yetAnotherTag"),
                Type.of("myType"));
        query = Query.fromItem(
                QueryItem.of(Tag.of("myTag").andTag("otherTag").andTag("yetAnotherTag"),
                        Type.of("myType"))
        );
        query = Query.of(Tag.of("myTag"),
                Type.of("myType").orType("otherType").orType("yetAnotherType"));
        query = Query.fromItem(
                QueryItem.of(Tag.of("myTag"),
                        Type.of("myType").orType("otherType").orType("yetAnotherType"))
        );

        // synonyms - multiple items with single tag and single type
        query = Query.either("myTag","myType")
                .or("otherTag","otherType")
                .or("yetAnotherTag","yetAnotherType")
                .build();
        query = Query.fromItems(
                QueryItem.of("myTag","myType")
                        .orItemOf("otherTag","otherType")
                        .orItemOf("yetAnotherTag","yetAnotherType")
        );
        query = Query.or(
                QueryItem.of(Tag.of("myTag"), Type.of("myType")),
                QueryItem.of(Tag.of("otherTag"), Type.of("otherType")),
                QueryItem.of(Tag.of("yetAnotherTag"), Type.of("yetAnotherType"))
        );
        query = Query.fromItems(
                QueryItems.or(
                        QueryItem.of(Tag.of("myTag"), Type.of("myType")),
                        QueryItem.of(Tag.of("otherTag"), Type.of("otherType")),
                        QueryItem.of(Tag.of("yetAnotherTag"), Type.of("yetAnotherType"))
                )
        );

    }
}
