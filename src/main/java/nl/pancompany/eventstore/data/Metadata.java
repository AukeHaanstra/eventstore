package nl.pancompany.eventstore.data;

import java.util.HashMap;
import java.util.Map;

public class Metadata extends HashMap<String, String> {

    public Metadata() {
    }

    public Metadata(Map<? extends String, ? extends String> m) {
        super(m);
    }

    public static Metadata of(String key1, String value1) {
        return new Metadata(Map.of(key1, value1));
    }


    public static Metadata of(String key1, String value1, String key2, String value2) {
        return new Metadata(Map.of(key1, value1, key2, value2));
    }


    public static Metadata of(String key1, String value1, String key2, String value2, String key3, String value3) {
        return new Metadata(Map.of(key1, value1, key2, value2, key3, value3));
    }


    public static Metadata of(String key1, String value1, String key2, String value2, String key3, String value3, String key4, String value4) {
        return new Metadata(Map.of(key1, value1, key2, value2, key3, value3, key4, value4));
    }


    public static Metadata of(String key1, String value1, String key2, String value2, String key3, String value3, String key4, String value4, String key5, String value5) {
        return new Metadata(Map.of(key1, value1, key2, value2, key3, value3, key4, value4, key5, value5));
    }


    public static Metadata of(String key1, String value1, String key2, String value2, String key3, String value3, String key4, String value4, String key5, String value5,
                       String key6, String value6) {
        return new Metadata(Map.of(key1, value1, key2, value2, key3, value3, key4, value4, key5, value5,
                key6, value6));
    }


    public static Metadata of(String key1, String value1, String key2, String value2, String key3, String value3, String key4, String value4, String key5, String value5,
                       String key6, String value6, String key7, String value7) {
        return new Metadata(Map.of(key1, value1, key2, value2, key3, value3, key4, value4, key5, value5,
                key6, value6, key7, value7));
    }


    public static Metadata of(String key1, String value1, String key2, String value2, String key3, String value3, String key4, String value4, String key5, String value5,
                       String key6, String value6, String key7, String value7, String key8, String value8) {
        return new Metadata(Map.of(key1, value1, key2, value2, key3, value3, key4, value4, key5, value5,
                key6, value6, key7, value7, key8, value8));
    }


    public static Metadata of(String key1, String value1, String key2, String value2, String key3, String value3, String key4, String value4, String key5, String value5,
                       String key6, String value6, String key7, String value7, String key8, String value8, String key9, String value9) {
        return new Metadata(Map.of(key1, value1, key2, value2, key3, value3, key4, value4, key5, value5,
                key6, value6, key7, value7, key8, value8, key9, value9));
    }


    public static Metadata of(String key1, String value1, String key2, String value2, String key3, String value3, String key4, String value4, String key5, String value5,
                       String key6, String value6, String key7, String value7, String key8, String value8, String key9, String value9, String key10, String value10) {
        return new Metadata(Map.of(key1, value1, key2, value2, key3, value3, key4, value4, key5, value5,
                key6, value6, key7, value7, key8, value8, key9, value9, key10, value10));
    }
}
