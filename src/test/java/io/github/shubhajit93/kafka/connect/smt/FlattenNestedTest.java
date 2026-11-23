package io.github.shubhajit93.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FlattenNestedTest {

    private static final Schema ADDRESS = SchemaBuilder.struct().name("Address")
            .field("city", Schema.STRING_SCHEMA)
            .field("zip", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    private static final Schema CUSTOMER = SchemaBuilder.struct().name("Customer")
            .field("name", Schema.STRING_SCHEMA)
            .field("address", ADDRESS)
            .build();

    private static final Schema ITEM = SchemaBuilder.struct().name("Item")
            .field("sku", Schema.STRING_SCHEMA)
            .field("qty", Schema.INT32_SCHEMA)
            .build();

    private static final Schema ORDER = SchemaBuilder.struct().name("Order")
            .field("id", Schema.INT64_SCHEMA)
            .field("customer", CUSTOMER)
            .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
            .field("items", SchemaBuilder.array(ITEM).build())
            .field("attributes", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().build())
            .build();

    private FlattenNested<SinkRecord> transform;

    @AfterEach
    void tearDown() {
        if (transform != null) {
            transform.close();
        }
    }

    private FlattenNested<SinkRecord> value(Map<String, String> config) {
        transform = new FlattenNested.Value<>();
        transform.configure(config);
        return transform;
    }

    private static Struct order() {
        Struct address = new Struct(ADDRESS).put("city", "Dhaka").put("zip", "1212");
        Struct customer = new Struct(CUSTOMER).put("name", "Ana").put("address", address);
        List<Struct> items = Arrays.asList(
                new Struct(ITEM).put("sku", "A-1").put("qty", 2),
                new Struct(ITEM).put("sku", "B-7").put("qty", 1));
        Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put("channel", "web");
        return new Struct(ORDER).put("id", 42L).put("customer", customer)
                .put("tags", Arrays.asList("new", "priority")).put("items", items).put("attributes", attributes);
    }

    private static SinkRecord record(Schema schema, Object value) {
        return new SinkRecord("orders", 0, Schema.STRING_SCHEMA, "k", schema, value, 0L);
    }

    @Test
    void flattensNestedStructsWithDelimiter() {
        SinkRecord out = value(Map.of()).apply(record(ORDER, order()));

        Struct flat = (Struct) out.value();
        assertThat(out.valueSchema().fields()).extracting(f -> f.name()).containsExactly(
                "id", "customer_name", "customer_address_city", "customer_address_zip", "tags", "items", "attributes");
        assertThat(flat.get("customer_name")).isEqualTo("Ana");
        assertThat(flat.get("customer_address_city")).isEqualTo("Dhaka");
        assertThat(flat.get("id")).isEqualTo(42L);
        assertThat(out.valueSchema().name()).isEqualTo("Order");
    }

    @Test
    void rendersArraysAndMapsAsJsonByDefault() {
        Struct flat = (Struct) value(Map.of()).apply(record(ORDER, order())).value();

        assertThat(flat.get("tags")).isEqualTo("[\"new\",\"priority\"]");
        assertThat(flat.get("items")).isEqualTo("[{\"sku\":\"A-1\",\"qty\":2},{\"sku\":\"B-7\",\"qty\":1}]");
        assertThat(flat.get("attributes")).isEqualTo("{\"channel\":\"web\"}");
    }

    @Test
    void joinModeJoinsPrimitiveArraysButKeepsJsonForStructs() {
        Struct flat = (Struct) value(Map.of("array.mode", "JOIN", "array.join.separator", "|"))
                .apply(record(ORDER, order())).value();

        assertThat(flat.get("tags")).isEqualTo("new|priority");
        assertThat(flat.get("items")).asString().startsWith("[{\"sku\":\"A-1\"");
    }

    @Test
    void customDelimiter() {
        Struct flat = (Struct) value(Map.of("delimiter", ".")).apply(record(ORDER, order())).value();

        assertThat(flat.get("customer.address.city")).isEqualTo("Dhaka");
    }

    @Test
    void childrenOfOptionalStructBecomeOptionalAndNullWhenParentIsNull() {
        Schema optionalAddress = SchemaBuilder.struct().name("Address").optional()
                .field("city", Schema.STRING_SCHEMA).build();
        Schema schema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).field("address", optionalAddress).build();

        SinkRecord out = value(Map.of()).apply(record(schema, new Struct(schema).put("id", 1)));

        assertThat(out.valueSchema().field("address_city").schema().isOptional()).isTrue();
        assertThat(((Struct) out.value()).get("address_city")).isNull();
    }

    @Test
    void nullArrayStaysNull() {
        Struct input = order().put("tags", null);

        Struct flat = (Struct) value(Map.of()).apply(record(ORDER, input)).value();

        assertThat(flat.get("tags")).isNull();
        assertThat(flat.schema().field("tags").schema()).isEqualTo(Schema.OPTIONAL_STRING_SCHEMA);
    }

    @Test
    void keepsLogicalTypesOnFlattenedFieldsAndRendersThemInsideArrays() {
        Schema amount = Decimal.schema(2);
        Schema payment = SchemaBuilder.struct().field("amount", amount).field("at", Timestamp.SCHEMA).build();
        Schema schema = SchemaBuilder.struct()
                .field("payment", payment)
                .field("history", SchemaBuilder.array(Timestamp.SCHEMA).build())
                .build();
        Date at = new Date(1_700_000_000_000L);
        Struct value = new Struct(schema)
                .put("payment", new Struct(payment).put("amount", new BigDecimal("19.90")).put("at", at))
                .put("history", List.of(at));

        SinkRecord out = value(Map.of()).apply(record(schema, value));

        assertThat(out.valueSchema().field("payment_amount").schema().name()).isEqualTo(Decimal.LOGICAL_NAME);
        assertThat(((Struct) out.value()).get("payment_amount")).isEqualTo(new BigDecimal("19.90"));
        assertThat(((Struct) out.value()).get("history")).isEqualTo("[\"2023-11-14T22:13:20Z\"]");
    }

    @Test
    void escapesStringsInJson() {
        Schema schema = SchemaBuilder.struct().field("notes", SchemaBuilder.array(Schema.STRING_SCHEMA).build()).build();
        Struct value = new Struct(schema).put("notes", List.of("say \"hi\"\n", "back\\slash", "\u0001"));

        Struct flat = (Struct) value(Map.of()).apply(record(schema, value)).value();

        assertThat(flat.get("notes")).isEqualTo("[\"say \\\"hi\\\"\\n\",\"back\\\\slash\",\"\\u0001\"]");
    }

    @Test
    void flattensSchemalessMaps() {
        Map<String, Object> address = new LinkedHashMap<>();
        address.put("city", "Dhaka");
        Map<String, Object> input = new LinkedHashMap<>();
        input.put("id", 7);
        input.put("address", address);
        input.put("tags", List.of("a", "b"));
        input.put("items", List.of(Map.of("sku", "A-1")));

        SinkRecord out = value(Map.of("array.mode", "join")).apply(record(null, input));

        @SuppressWarnings("unchecked")
        Map<String, Object> flat = (Map<String, Object>) out.value();
        assertThat(flat).containsEntry("id", 7).containsEntry("address_city", "Dhaka").containsEntry("tags", "a,b")
                .containsEntry("items", "[{\"sku\":\"A-1\"}]");
        assertThat(out.valueSchema()).isNull();
    }

    @Test
    void tombstonesPassThrough() {
        SinkRecord tombstone = record(ORDER, null);

        assertThat(value(Map.of()).apply(tombstone)).isSameAs(tombstone);
    }

    @Test
    void keyVariantFlattensOnlyTheKey() {
        transform = new FlattenNested.Key<>();
        transform.configure(Map.of());
        SinkRecord input = new SinkRecord("orders", 0, CUSTOMER,
                new Struct(CUSTOMER).put("name", "Ana").put("address", new Struct(ADDRESS).put("city", "Dhaka")),
                ORDER, order(), 0L);

        SinkRecord out = transform.apply(input);

        assertThat(((Struct) out.key()).get("address_city")).isEqualTo("Dhaka");
        assertThat(out.value()).isSameAs(input.value());
    }

    @Test
    void reusesTheFlattenedSchemaForTheSameInputSchema() {
        FlattenNested<SinkRecord> smt = value(Map.of());

        Schema first = smt.apply(record(ORDER, order())).valueSchema();
        Schema second = smt.apply(record(ORDER, order())).valueSchema();

        assertThat(second).isSameAs(first);
    }

    @Test
    void rejectsFieldNameCollisions() {
        Schema child = SchemaBuilder.struct().field("b", Schema.STRING_SCHEMA).build();
        Schema schema = SchemaBuilder.struct().field("a_b", Schema.STRING_SCHEMA).field("a", child).build();
        Struct value = new Struct(schema).put("a_b", "x").put("a", new Struct(child).put("b", "y"));

        assertThatThrownBy(() -> value(Map.of()).apply(record(schema, value)))
                .isInstanceOf(DataException.class).hasMessageContaining("'a_b' twice");
    }

    @Test
    void rejectsNonStructValues() {
        assertThatThrownBy(() -> value(Map.of()).apply(record(Schema.STRING_SCHEMA, "plain")))
                .isInstanceOf(DataException.class);
    }

    @Test
    void rejectsInvalidConfiguration() {
        Map<String, String> bad = new HashMap<>();
        bad.put("array.mode", "explode");

        assertThatThrownBy(() -> value(bad)).isInstanceOf(ConfigException.class);
        assertThatThrownBy(() -> value(Map.of("delimiter", ""))).isInstanceOf(ConfigException.class);
    }
}
