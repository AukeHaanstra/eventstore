package nl.pancompany.eventstore;

import org.junit.jupiter.api.Test;

import java.util.List;

public class QueryApiTest {

    @Test
    public void createQueryTest() {
        // synonyms - all
        Query.all();
        Query.fromItem(QueryItem.all());
        Query.fromItems(QueryItems.all());
        Query.of(Tags.all(), Types.all());
        Query.fromItem(
                QueryItem.of(Tags.all(), Types.all())
        );
        Query.fromItem(
                QueryItem.taggedWith(Tags.all()).andHavingType(Types.all())
        );
        Query.fromItem(
                QueryItem.havingType(Types.all()).andTaggedWith(Tags.all())
        );

        // synonyms - one item with one tag and one type
        Query.of("myTag","myType");
        Query.of(Tag.of("myTag"), Type.of("myType"));
        Query.fromItem(
                QueryItem.of("myTag","myType")
        );
        Query.fromItem(
                QueryItem.of(Tag.of("myTag"), Type.of("myType"))
        );

        List<String> tagStrings = List.of("myTag", "otherTag");
        List<String> typeStrings = List.of("myType", "otherType");
        // synonyms - one item with multiple tags and multiple types
        Query.of(Tags.and("myTag", "otherTag"), Types.or("myType", "otherType"));
        Query.of(Tag.of("myTag").andTag("otherTag").andTag("yetAnotherTag"),
                Type.of("myType").orType("otherType").orType("yetAnotherType"));
        Query.fromItem(
                QueryItem.of(Tags.and("myTag", "otherTag"), Types.or("myType", "otherType"))
        );
        Query.fromItem(
                QueryItem.of(Tags.and(tagStrings), Types.or(typeStrings))
        );
        Query.fromItem(
                QueryItem.of(Tag.of("myTag").andTag("otherTag").andTag("yetAnotherTag"),
                        Type.of("myType").orType("otherType").orType("yetAnotherType"))
        );
        Query.fromItem(
                QueryItem.taggedWith("myTag", "otherTag").andHavingType("myType", "otherType")
        );
        Query.fromItem(
                QueryItem.taggedWith(tagStrings).andHavingType(typeStrings)
        );
        Query.fromItem(
                QueryItem.taggedWith(Tag.of("myTag").andTag("otherTag").andTag("yetAnotherTag"))
                        .andHavingType(Type.of("myType").orType("otherType").orType("yetAnotherType"))
        );
        Query.fromItem(
                QueryItem.taggedWith(Tag.of("myTag"), Tag.of("otherTag"), Tag.of("yetAnotherTag"))
                        .andHavingType(Type.of("myType"), Type.of("otherType"), Type.of("yetAnotherType"))
        );
        Query.fromItem(
                QueryItem.havingType("myType", "otherType").andTaggedWith("myTag", "otherTag")
        );
        Query.fromItem(
                QueryItem.havingType(typeStrings).andTaggedWith(tagStrings)
        );
        Query.fromItem(
                QueryItem.havingType(Type.of("myType").orType("otherType").orType("yetAnotherType"))
                        .andTaggedWith(Tag.of("myTag").andTag("otherTag").andTag("yetAnotherTag"))

        );
        Query.fromItem(
                QueryItem.havingType(Type.of("myType"), Type.of("otherType"), Type.of("yetAnotherType"))
                        .andTaggedWith(Tag.of("myTag"), Tag.of("otherTag"), Tag.of("yetAnotherTag"))

        );
        // variations - mixing with single tag or single type
        Query.fromItem(
                QueryItem.of(Tag.of("myTag").andTag("otherTag").andTag("yetAnotherTag"),
                        Type.of("myType"))
        );
        Query.fromItem(
                QueryItem.of(Tag.of("myTag"),
                        Type.of("myType").orType("otherType").orType("yetAnotherType"))
        );

        // synonyms - multiple items with single tag and single type
        Query.fromItems(
                QueryItem.of("myTag","myType")
                        .orItemOf("otherTag","otherType")
                        .orItemOf("yetAnotherTag","yetAnotherType")
        );
        Query.fromItems(
                QueryItems.or(
                        QueryItem.of(Tag.of("myTag"), Type.of("myType")),
                        QueryItem.of(Tag.of("otherTag"), Type.of("otherType")),
                        QueryItem.of(Tag.of("yetAnotherTag"), Type.of("yetAnotherType"))
                )
        );

    }
}
