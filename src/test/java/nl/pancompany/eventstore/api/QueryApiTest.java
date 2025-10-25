package nl.pancompany.eventstore.api;

import nl.pancompany.eventstore.*;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static nl.pancompany.eventstore.Query.all;
import static org.assertj.core.api.Assertions.assertThat;

public class QueryApiTest {

    record MyType(String data) {
    }

    record OtherType(String data) {
    }

    @Test
    public void createQueryTest() {
        Query query;
        // synonyms - all
        query = all();
        assertThat(query.isAll()).isTrue();
        query = Query.all();
        assertThat(query.isAll()).isTrue();
        query = Query.fromItem(QueryItem.all());
        assertThat(query.isAll()).isTrue();
        query = Query.fromItems(QueryItems.all());
        assertThat(query.isAll()).isTrue();
        query = Query.of(Tags.all(), Types.all());
        assertThat(query.isAll()).isTrue();
        query = Query.fromItem(
                QueryItem.of(Tags.all(), Types.all())
        );
        assertThat(query.isAll()).isTrue();
        query = Query.taggedWith(Tags.all()).build();
        assertThat(query.isAll()).isTrue();
        query = Query.fromItem(
                QueryItem.taggedWith(Tags.all()).build()
        );
        assertThat(query.isAll()).isTrue();
        query = Query.havingType(Types.all()).build();
        assertThat(query.isAll()).isTrue();
        query = Query.fromItem(
                QueryItem.havingType(Types.all()).build()
        );
        assertThat(query.isAll()).isTrue();
        query = Query.taggedWith(Tags.all()).andHavingType(Types.all());
        assertThat(query.isAll()).isTrue();
        query = Query.fromItem(
                QueryItem.taggedWith(Tags.all()).andHavingType(Types.all())
        );
        assertThat(query.isAll()).isTrue();
        query = Query.havingType(Types.all()).andTaggedWith(Tags.all());
        assertThat(query.isAll()).isTrue();
        query = Query.fromItem(
                QueryItem.havingType(Types.all()).andTaggedWith(Tags.all())
        );
        assertThat(query.isAll()).isTrue();

        Assertions.assertThatThrownBy(() -> Query.of("myTag", new Object() {}.getClass())).isInstanceOf(IllegalArgumentException.class);

        // synonyms - one item with one tag and one type
        query = Query.of("myTag","myType");
        assertSingleTagAndType(query);
        query = Query.of("myTag", MyType.class);
        assertThat(query.getQueryItems().iterator().next().types()).containsOnly(Type.of(MyType.class.getCanonicalName()));
        query = Query.of(Tag.of("myTag"), Type.of("myType"));
        assertSingleTagAndType(query);
        query = Query.of(Tag.of("myTag"), Type.of(MyType.class));
        assertThat(query.getQueryItems().iterator().next().types()).containsOnly(Type.of(MyType.class.getCanonicalName()));
        query = Query.fromItem(
                QueryItem.of("myTag","myType")
        );
        assertSingleTagAndType(query);
        query = Query.fromItem(
                QueryItem.of("myTag", MyType.class)
        );
        assertThat(query.getQueryItems().iterator().next().types()).containsOnly(Type.of(MyType.class.getCanonicalName()));
        query = Query.of(Tag.of("myTag"), Type.of("myType"));
        assertSingleTagAndType(query);
        query = Query.fromItem(
                QueryItem.of(Tag.of("myTag"), Type.of("myType"))
        );
        assertSingleTagAndType(query);

        Set<String> tagStrings = Set.of("myTag", "otherTag");
        Set<String> typeStrings = Set.of("myType", "otherType");
        // synonyms - one item with multiple tags and multiple types
        query = Query.of(Tags.and("myTag", "otherTag"), Types.or("myType", "otherType"));
        assertTwoTagsAndTwoTypes(query);
        query = Query.fromItem(
                QueryItem.of(Tags.and("myTag", "otherTag"), Types.or("myType", "otherType"))
        );
        assertTwoTagsAndTwoTypes(query);
        query = Query.of(Tags.and(tagStrings), Types.or(typeStrings));
        assertTwoTagsAndTwoTypes(query);
        query = Query.fromItem(
                QueryItem.of(Tags.and(tagStrings), Types.or(typeStrings))
        );
        assertTwoTagsAndTwoTypes(query);
        query = Query.of(Tag.of("myTag").andTag("otherTag").andTag("yetAnotherTag"),
                Type.of("myType").orType("otherType").orType("yetAnotherType"));
        assertThreeTagsAndThreeTypes(query);
        query = Query.fromItem(
                QueryItem.of(Tag.of("myTag").andTag("otherTag").andTag("yetAnotherTag"),
                        Type.of("myType").orType("otherType").orType("yetAnotherType"))
        );
        assertThreeTagsAndThreeTypes(query);
        query = Query.taggedWith("myTag", "otherTag").andHavingType("myType", "otherType");
        query = Query.fromItem(
                QueryItem.taggedWith("myTag", "otherTag").andHavingType("myType", "otherType")
        );
        assertTwoTagsAndTwoTypes(query);
        query = Query.taggedWith(tagStrings).andHavingType(typeStrings);
        query = Query.fromItem(
                QueryItem.taggedWith(tagStrings).andHavingType(typeStrings)
        );
        assertTwoTagsAndTwoTypes(query);
        query = Query.taggedWith(Tag.of("myTag").andTag("otherTag").andTag("yetAnotherTag"))
                .andHavingType(Type.of("myType").orType("otherType").orType("yetAnotherType"));
        assertThreeTagsAndThreeTypes(query);
        query = Query.fromItem(
                QueryItem.taggedWith(Tag.of("myTag").andTag("otherTag").andTag("yetAnotherTag"))
                        .andHavingType(Type.of("myType").orType("otherType").orType("yetAnotherType"))
        );
        assertThreeTagsAndThreeTypes(query);
        query = Query.taggedWith(Tag.of("myTag"), Tag.of("otherTag"), Tag.of("yetAnotherTag"))
                .andHavingType(Type.of("myType"), Type.of("otherType"), Type.of("yetAnotherType"));
        assertThreeTagsAndThreeTypes(query);
        query = Query.fromItem(
                QueryItem.taggedWith(Tag.of("myTag"), Tag.of("otherTag"), Tag.of("yetAnotherTag"))
                        .andHavingType(Type.of("myType"), Type.of("otherType"), Type.of("yetAnotherType"))
        );
        assertThreeTagsAndThreeTypes(query);
        query = Query.havingType("myType", "otherType").andTaggedWith("myTag", "otherTag");
        assertTwoTagsAndTwoTypes(query);
        query = Query.havingType(MyType.class, OtherType.class).andTaggedWith("myTag", "otherTag");
        assertTwoTagsAndTwoDerivedTypes(query);
        query = Query.fromItem(
                QueryItem.havingType("myType", "otherType").andTaggedWith("myTag", "otherTag")
        );
        assertTwoTagsAndTwoTypes(query);
        query = Query.fromItem(
                QueryItem.havingType(MyType.class, OtherType.class).andTaggedWith("myTag", "otherTag")
        );
        assertTwoTagsAndTwoDerivedTypes(query);
        query = Query.havingType(typeStrings).andTaggedWith(tagStrings);
        assertTwoTagsAndTwoTypes(query);
        query = Query.fromItem(
                QueryItem.havingType(typeStrings).andTaggedWith(tagStrings)
        );
        assertTwoTagsAndTwoTypes(query);
        query = Query.havingType(Type.of("myType").orType("otherType").orType("yetAnotherType"))
                .andTaggedWith(Tag.of("myTag").andTag("otherTag").andTag("yetAnotherTag"));
        assertThreeTagsAndThreeTypes(query);
        query = Query.fromItem(
                QueryItem.havingType(Type.of("myType").orType("otherType").orType("yetAnotherType"))
                        .andTaggedWith(Tag.of("myTag").andTag("otherTag").andTag("yetAnotherTag"))

        );
        assertThreeTagsAndThreeTypes(query);
        query = Query.havingType(Type.of("myType"), Type.of("otherType"), Type.of("yetAnotherType"))
                .andTaggedWith(Tag.of("myTag"), Tag.of("otherTag"), Tag.of("yetAnotherTag"));
        assertThreeTagsAndThreeTypes(query);
        query = Query.fromItem(
                QueryItem.havingType(Type.of("myType"), Type.of("otherType"), Type.of("yetAnotherType"))
                        .andTaggedWith(Tag.of("myTag"), Tag.of("otherTag"), Tag.of("yetAnotherTag"))

        );
        assertThreeTagsAndThreeTypes(query);
        // variations - mixing with single tag or single type
        query = Query.of(Tag.of("myTag").andTag("otherTag").andTag("yetAnotherTag"),
                Type.of("myType"));
        assertThat(query.getQueryItems().iterator().next().tags()).containsOnly(Tag.of("myTag"), Tag.of("otherTag"), Tag.of("yetAnotherTag"));
        assertThat(query.getQueryItems().iterator().next().types()).containsOnly(Type.of("myType"));
        query = Query.fromItem(
                QueryItem.of(Tag.of("myTag").andTag("otherTag").andTag("yetAnotherTag"),
                        Type.of("myType"))
        );
        assertThat(query.getQueryItems().iterator().next().tags()).containsOnly(Tag.of("myTag"), Tag.of("otherTag"), Tag.of("yetAnotherTag"));
        assertThat(query.getQueryItems().iterator().next().types()).containsOnly(Type.of("myType"));
        query = Query.of(Tag.of("myTag"),
                Type.of("myType").orType("otherType").orType("yetAnotherType"));
        assertThat(query.getQueryItems().iterator().next().tags()).containsOnly(Tag.of("myTag"));
        assertThat(query.getQueryItems().iterator().next().types()).containsOnly(Type.of("myType"), Type.of("otherType"), Type.of("yetAnotherType"));
        query = Query.fromItem(
                QueryItem.of(Tag.of("myTag"),
                        Type.of("myType").orType("otherType").orType("yetAnotherType"))
        );
        assertThat(query.getQueryItems().iterator().next().tags()).containsOnly(Tag.of("myTag"));
        assertThat(query.getQueryItems().iterator().next().types()).containsOnly(Type.of("myType"), Type.of("otherType"), Type.of("yetAnotherType"));


        // synonyms - multiple items with single tag and single type
        query = Query.either("myTag","myType")
                .or("otherTag","otherType")
                .or("yetAnotherTag","yetAnotherType")
                .build();
        assertThreeQueryItems(query);
        query = Query.fromItems(
                QueryItem.of("myTag","myType")
                        .orItemOf("otherTag","otherType")
                        .orItemOf("yetAnotherTag","yetAnotherType")
        );
        assertThreeQueryItems(query);
        query = Query.or(
                QueryItem.of(Tag.of("myTag"), Type.of("myType")),
                QueryItem.of(Tag.of("otherTag"), Type.of("otherType")),
                QueryItem.of(Tag.of("yetAnotherTag"), Type.of("yetAnotherType"))
        );
        assertThreeQueryItems(query);
        query = Query.fromItems(
                QueryItems.or(
                        QueryItem.of(Tag.of("myTag"), Type.of("myType")),
                        QueryItem.of(Tag.of("otherTag"), Type.of("otherType")),
                        QueryItem.of(Tag.of("yetAnotherTag"), Type.of("yetAnotherType"))
                )
        );
        assertThreeQueryItems(query);
    }

    private static void assertSingleTagAndType(Query query) {
        assertThat(query.getQueryItems().iterator().next().tags()).containsOnly(Tag.of("myTag"));
        assertThat(query.getQueryItems().iterator().next().types()).containsOnly(Type.of("myType"));
    }

    private static void assertTwoTagsAndTwoTypes(Query query) {
        assertThat(query.getQueryItems().iterator().next().tags()).containsOnly(Tag.of("myTag"), Tag.of("otherTag"));
        assertThat(query.getQueryItems().iterator().next().types()).containsOnly(Type.of("myType"), Type.of("otherType"));
    }

    private static void assertTwoTagsAndTwoDerivedTypes(Query query) {
        assertThat(query.getQueryItems().iterator().next().tags()).containsOnly(Tag.of("myTag"), Tag.of("otherTag"));
        assertThat(query.getQueryItems().iterator().next().types()).containsOnly(
                Type.of(MyType.class.getCanonicalName()),
                Type.of(OtherType.class.getCanonicalName()));
    }

    private static void assertThreeTagsAndThreeTypes(Query query) {
        assertThat(query.getQueryItems().iterator().next().tags()).containsOnly(Tag.of("myTag"), Tag.of("otherTag"), Tag.of("yetAnotherTag"));
        assertThat(query.getQueryItems().iterator().next().types()).containsOnly(Type.of("myType"), Type.of("otherType"), Type.of("yetAnotherType"));
    }

    private static void assertThreeQueryItems(Query query) {
        assertThat(query.getQueryItems()).containsOnly(
                QueryItem.of(Tag.of("myTag"), Type.of("myType")),
                QueryItem.of(Tag.of("otherTag"), Type.of("otherType")),
                QueryItem.of(Tag.of("yetAnotherTag"), Type.of("yetAnotherType"))
        );
    }
}
