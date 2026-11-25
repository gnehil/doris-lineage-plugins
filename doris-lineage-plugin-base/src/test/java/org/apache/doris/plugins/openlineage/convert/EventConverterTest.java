package org.apache.doris.plugins.openlineage.convert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.plugins.openlineage.convert.LineageInput.ColumnEdge;
import org.apache.doris.plugins.openlineage.convert.LineageInput.ColumnRef;
import org.apache.doris.plugins.openlineage.convert.LineageInput.DatasetId;
import org.apache.doris.plugins.openlineage.convert.LineageInput.DatasetIndirect;
import org.apache.doris.plugins.openlineage.convert.LineageInput.DatasetSchema;
import org.apache.doris.plugins.openlineage.convert.LineageInput.SchemaField;
import org.apache.doris.plugins.openlineage.convert.LineageInput.Kind;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventConverterTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final DatasetId ORDERS = new DatasetId("internal", "sales", "orders");
    private static final DatasetId CUSTOMERS = new DatasetId("internal", "sales", "customers");
    private static final DatasetId SUMMARY = new DatasetId("hive", "dw", "order_summary");
    private static final UUID QUERY_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");

    @Test
    void emitsStartThenCompleteWithStableIdentityAndColumnLineage() throws Exception {
        EventEncoder.ConversionResult result = new EventConverter(UUID::randomUUID).convert(sample(QUERY_ID.toString(), 1_000L, 30L));

        assertTrue(result.lineageComplete());
        assertEquals("doris", result.startTimeSource());
        assertEquals(2, result.events().size());
        JsonNode start = JSON.readTree(result.events().get(0).body());
        JsonNode complete = JSON.readTree(result.events().get(1).body());
        assertEquals("START", start.get("eventType").asText());
        assertEquals("COMPLETE", complete.get("eventType").asText());
        assertTrue(start.get("schemaURL").asText().contains("2-0-2/OpenLineage.json"));
        assertEquals(start.get("schemaURL").asText(), complete.get("schemaURL").asText());
        assertEquals(QUERY_ID.toString(), start.get("run").get("runId").asText());
        assertEquals(start.get("run").get("runId").asText(), complete.get("run").get("runId").asText());
        assertEquals("doris://warehouse-a", start.get("job").get("namespace").asText());
        assertEquals("hive.dw.order_summary", start.get("job").get("name").asText());
        assertFalse(start.toString().contains("fe-host"));
        assertEquals(Instant.ofEpochMilli(1_000L), Instant.parse(start.get("eventTime").asText()));
        assertEquals(Instant.ofEpochMilli(1_030L), Instant.parse(complete.get("eventTime").asText()));

        JsonNode inputs = start.get("inputs");
        assertEquals("internal.sales.orders", inputs.get(0).get("name").asText());
        assertEquals("amt", inputs.get(0).at("/facets/schema/fields/0/name").asText());
        assertEquals("DECIMAL(10,2)", inputs.get(0).at("/facets/schema/fields/0/type").asText());
        assertEquals("region", inputs.get(1).at("/facets/schema/fields/0/name").asText());
        assertEquals("amount", start.at("/outputs/0/facets/schema/fields/0/name").asText());
        assertEquals("DECIMAL(10,2)", start.at("/outputs/0/facets/schema/fields/0/type").asText());
        assertEquals("flag", start.at("/outputs/0/facets/schema/fields/2/name").asText());
        assertEquals("TINYINT", start.at("/outputs/0/facets/schema/fields/2/type").asText());
        assertTrue(start.at("/outputs/0/facets/columnLineage/fields/flag").isMissingNode()
                || start.at("/outputs/0/facets/columnLineage/fields/flag/inputFields").toString().contains("FILTER"));
        assertEquals("internal.sales.customers", inputs.get(1).get("name").asText());
        JsonNode amount = start.at("/outputs/0/facets/columnLineage/fields/amount/inputFields");
        assertEquals("internal.sales.orders", amount.get(0).get("name").asText());
        assertEquals("amt", amount.get(0).get("field").asText());
        assertEquals("DIRECT", amount.get(0).at("/transformations/0/type").asText());
        assertEquals("TRANSFORMATION", amount.get(0).at("/transformations/0/subtype").asText());
        assertEquals("qty", amount.get(1).get("field").asText());
        JsonNode total = start.at("/outputs/0/facets/columnLineage/fields/total/inputFields/0");
        assertEquals("INDIRECT", total.at("/transformations/0/type").asText());
        assertEquals("AGGREGATION", total.at("/transformations/0/subtype").asText());
        JsonNode datasetLevel = start.at("/outputs/0/facets/columnLineage/dataset");
        assertEquals("internal.sales.customers", datasetLevel.get(0).get("name").asText());
        assertEquals("region", datasetLevel.get(0).get("field").asText());
        assertEquals("FILTER", datasetLevel.get(0).at("/transformations/0/subtype").asText());
        assertTrue(start.at("/outputs/0/facets/columnLineage/fields/amount/inputFields").toString().contains("\"field\":\"region\"")
                || start.at("/outputs/0/facets/columnLineage/fields/amount/inputFields").toString().contains("\"field\": \"region\""));
        ColumnLineageSchema.assertValid(start.at("/outputs/0/facets/columnLineage"));
    }

    @Test
    void keepsDirectAndIndirectInputsOnTheSameOutputColumn() throws Exception {
        LineageInput input = new LineageInput(
                QUERY_ID.toString(),
                "warehouse-a",
                SUMMARY,
                List.of(ORDERS),
                List.of(
                        new ColumnEdge("amount", Kind.DIRECT, "IDENTITY", List.of(new ColumnRef(ORDERS, "amt"))),
                        new ColumnEdge("amount", Kind.INDIRECT, "WINDOW", List.of(new ColumnRef(ORDERS, "order_id")))),
                List.of(),
                List.of(),
                List.of("amount"),
                1_000L,
                1L,
                2_500L);
        JsonNode start = JSON.readTree(new EventConverter(UUID::randomUUID).convert(input).events().get(0).body());
        JsonNode inputs = start.at("/outputs/0/facets/columnLineage/fields/amount/inputFields");
        assertEquals(2, inputs.size());
        assertEquals("amt", inputs.get(0).get("field").asText());
        assertEquals("DIRECT", inputs.get(0).at("/transformations/0/type").asText());
        assertEquals("order_id", inputs.get(1).get("field").asText());
        assertEquals("INDIRECT", inputs.get(1).at("/transformations/0/type").asText());
        assertEquals("WINDOW", inputs.get(1).at("/transformations/0/subtype").asText());
        ColumnLineageSchema.assertValid(start.at("/outputs/0/facets/columnLineage"));
    }

    @Test
    void emitsEmptyFieldsWhenOnlyDatasetLevelLineageExists() throws Exception {
        LineageInput input = new LineageInput(
                QUERY_ID.toString(),
                "warehouse-a",
                SUMMARY,
                List.of(CUSTOMERS),
                List.of(),
                List.of(new DatasetIndirect(CUSTOMERS, "region", "FILTER")),
                List.of(DatasetSchema.ready(SUMMARY, List.of(new SchemaField("amount", "DECIMAL(10,2)"), new SchemaField("b", "INT")))),
                List.of("amount"),
                1_000L,
                1L,
                2_500L);
        JsonNode facet = JSON.readTree(new EventConverter(UUID::randomUUID).convert(input).events().get(0).body())
                .at("/outputs/0/facets/columnLineage");
        assertEquals("region", facet.at("/fields/amount/inputFields/0/field").asText());
        assertEquals("FILTER", facet.at("/fields/amount/inputFields/0/transformations/0/subtype").asText());
        assertTrue(facet.at("/fields/b").isMissingNode());
        assertEquals("b", JSON.readTree(new EventConverter(UUID::randomUUID).convert(input).events().get(0).body())
                .at("/outputs/0/facets/schema/fields/1/name").asText());
        assertEquals("region", facet.at("/dataset/0/field").asText());
        ColumnLineageSchema.assertValid(facet);
    }

    @Test
    void derivesAStableRunIdWhenTheQueryIdIsNotAUuid() {
        EventConverter converter = new EventConverter(UUID::randomUUID);
        LineageInput input = sample("query-17", 1_000L, 0L);
        EventEncoder.ConversionResult once = converter.convert(input);
        EventEncoder.ConversionResult twice = converter.convert(input);
        assertEquals(runId(once), runId(twice));
        assertFalse(runId(once).equals(QUERY_ID.toString()));
        UUID expected = RunIds.version5(RunIds.NAMESPACE, RunIds.nameBytes("doris://warehouse-a", "hive.dw.order_summary", "query-17"));
        assertEquals(expected.toString(), runId(once));
        assertEquals(runId(once), runId(new EventConverter(UUID::randomUUID).convert(input)));
    }

    @Test
    void usesOneRandomRunIdWhenTheQueryIdIsMissing() throws Exception {
        UUID random = UUID.fromString("22222222-2222-2222-2222-222222222222");
        EventEncoder.ConversionResult result = new EventConverter(() -> random).convert(sample("  ", null, null));
        JsonNode start = JSON.readTree(result.events().get(0).body());
        JsonNode complete = JSON.readTree(result.events().get(1).body());
        assertEquals(random.toString(), start.get("run").get("runId").asText());
        assertEquals(random.toString(), complete.get("run").get("runId").asText());
        assertEquals("receive_time", result.startTimeSource());
        assertEquals(start.get("eventTime").asText(), complete.get("eventTime").asText());
    }

    @Test
    void treatsDorisTimestampAsQueryEnd() throws Exception {
        DorisEventTimes.Window window = DorisEventTimes.from(12_000_010L, 10_000L);
        assertEquals(11_990_010L, window.startMs());
        assertEquals(10_000L, window.durationMs());
        EventEncoder.ConversionResult result = new EventConverter(UUID::randomUUID).convert(sample(
                QUERY_ID.toString(), window.startMs(), window.durationMs()));
        JsonNode start = JSON.readTree(result.events().get(0).body());
        JsonNode complete = JSON.readTree(result.events().get(1).body());
        assertEquals(Instant.ofEpochMilli(11_990_010L), Instant.parse(start.get("eventTime").asText()));
        assertEquals(Instant.ofEpochMilli(12_000_010L), Instant.parse(complete.get("eventTime").asText()));
        assertEquals(new DorisEventTimes.Window(null, null), DorisEventTimes.from(DorisEventTimes.UNKNOWN, 10L));
        DorisEventTimes.Window knownEnd = DorisEventTimes.from(9_000L, DorisEventTimes.UNKNOWN);
        assertEquals(9_000L, knownEnd.startMs());
        assertEquals(0L, knownEnd.durationMs());
    }

    @Test
    void fallsBackWhenDurationIsNegative() {
        EventEncoder.ConversionResult result = new EventConverter(UUID::randomUUID).convert(sample(QUERY_ID.toString(), 5_000L, -1L));
        assertEquals("receive_time", result.startTimeSource());
        assertTrue(result.diagnostics().stream().anyMatch(d -> d.reason().equals("start_time_source=receive_time")));
    }

    @Test
    void omitsUnresolvedColumnSourcesWithoutInventingAnEdge() throws Exception {
        LineageInput input = new LineageInput(
                QUERY_ID.toString(),
                "warehouse-a",
                SUMMARY,
                List.of(ORDERS),
                List.of(new ColumnEdge("amount", Kind.DIRECT, null, List.of(new ColumnRef(null, "amt")))),
                List.of(),
                List.of(),
                List.of("amount"),
                1_000L,
                1L,
                9_000L);
        EventEncoder.ConversionResult result = new EventConverter(UUID::randomUUID).convert(input);
        assertFalse(result.lineageComplete());
        assertEquals("amount", result.diagnostics().get(0).targetColumn());
        JsonNode start = JSON.readTree(result.events().get(0).body());
        assertTrue(start.at("/outputs/0/facets/columnLineage/fields/amount").isMissingNode());
        assertFalse(start.toString().contains("\"field\":\"amt\""));
    }

    @Test
    void marksMissingSourcesIncomplete() {
        EventConverter converter = new EventConverter(UUID::randomUUID);
        for (List<ColumnRef> sources : new List[] {List.of(), null}) {
            LineageInput input = new LineageInput(
                    QUERY_ID.toString(),
                    "warehouse-a",
                    SUMMARY,
                    List.of(ORDERS),
                    List.of(new ColumnEdge("amount", Kind.DIRECT, null, sources)),
                    List.of(),
                    List.of(),
                    List.of("amount"),
                    1_000L,
                    1L,
                    9_000L);
            EventEncoder.ConversionResult result = converter.convert(input);
            assertFalse(result.lineageComplete());
            assertEquals(QUERY_ID.toString(), result.diagnostics().get(0).queryId());
            assertEquals("amount", result.diagnostics().get(0).targetColumn());
            assertEquals("column source is unresolved", result.diagnostics().get(0).reason());
        }
    }

    @Test
    void constantProjectionWithoutEdgesKeepsLineageComplete() throws Exception {
        LineageInput constantOnly = new LineageInput(
                QUERY_ID.toString(),
                "warehouse-a",
                SUMMARY,
                List.of(ORDERS),
                List.of(),
                List.of(),
                List.of(),
                List.of("literal_only"),
                1_000L,
                1L,
                9_000L);
        EventEncoder.ConversionResult constants = new EventConverter(UUID::randomUUID).convert(constantOnly);
        assertTrue(constants.lineageComplete());
        assertTrue(constants.diagnostics().isEmpty());
        JsonNode start = JSON.readTree(constants.events().get(0).body());
        assertTrue(start.at("/outputs/0/facets/columnLineage").isMissingNode());
    }

    @Test
    void skipsPerColumnExpansionWhenItWouldBeHuge() throws Exception {
        List<String> outputs = new java.util.ArrayList<>();
        List<DatasetIndirect> indirect = new java.util.ArrayList<>();
        for (int i = 0; i < 200; i++) {
            outputs.add("c" + i);
            indirect.add(new DatasetIndirect(ORDERS, "s" + i, "GROUP_BY"));
        }
        LineageInput input = new LineageInput(
                QUERY_ID.toString(), "warehouse-a", SUMMARY, List.of(ORDERS), List.of(), indirect,
                List.of(DatasetSchema.ready(SUMMARY, List.of(new SchemaField("c0", "INT")))),
                outputs, 1_000L, 1L, 2_500L);
        EventEncoder.ConversionResult result = new EventConverter(UUID::randomUUID).convert(input);
        JsonNode facet = JSON.readTree(result.events().get(0).body()).at("/outputs/0/facets/columnLineage");
        assertTrue(facet.at("/fields/c0").isMissingNode());
        assertEquals("s0", facet.at("/dataset/0/field").asText());
        assertTrue(result.diagnostics().stream().anyMatch(d -> d.reason().contains("skipped per-column expansion")));
    }

    @Test
    void omitsSchemaFacetWhenSchemaIsUnavailable() throws Exception {
        LineageInput input = new LineageInput(
                QUERY_ID.toString(), "warehouse-a", SUMMARY, List.of(ORDERS),
                List.of(new ColumnEdge("amount", Kind.DIRECT, "IDENTITY", List.of(new ColumnRef(ORDERS, "amt")))),
                List.of(),
                List.of(DatasetSchema.unavailable(SUMMARY), DatasetSchema.ready(ORDERS, List.of())),
                List.of("amount"), 1_000L, 1L, 2_500L);
        EventEncoder.ConversionResult result = new EventConverter(UUID::randomUUID).convert(input);
        JsonNode start = JSON.readTree(result.events().get(0).body());
        assertTrue(start.at("/outputs/0/facets/schema").isMissingNode());
        assertTrue(start.at("/inputs/0/facets/schema").isMissingNode());
        assertEquals("amt", start.at("/outputs/0/facets/columnLineage/fields/amount/inputFields/0/field").asText());
        assertTrue(result.diagnostics().stream().anyMatch(d -> d.reason().equals("schema unavailable")));
    }

    @Test
    void doesNotSendWhenTheOutputIdentityIsMissing() {
        LineageInput input = sample(QUERY_ID.toString(), 1_000L, 1L);
        LineageInput broken = new LineageInput(
                input.queryId(), input.cluster(), new DatasetId("hive", " ", "order_summary"),
                input.inputs(), input.columns(), input.datasetIndirect(), input.schemas(), input.outputColumns(),
                input.startTimeMs(), input.durationMs(), input.receivedAtMs());
        EventEncoder.ConversionResult result = new EventConverter(UUID::randomUUID).convert(broken);
        assertTrue(result.events().isEmpty());
        assertFalse(result.lineageComplete());
    }

    @Test
    void dorisJsonPreservesTheSameTableAndColumnDependencies() throws Exception {
        LineageInput input = sample(QUERY_ID.toString(), 1_000L, 30L);
        var converted = new DorisJsonEncoder(UUID::randomUUID).convert(input);
        assertTrue(converted.lineageComplete());
        assertEquals(1, converted.events().size());
        var event = converted.events().get(0);
        assertEquals("LINEAGE", event.type());
        JsonNode json = JSON.readTree(event.body());
        JsonNode open = JSON.readTree(new EventConverter(UUID::randomUUID).convert(input).events().get(0).body());
        assertEquals(1, json.get("schemaVersion").asInt());
        assertEquals(QUERY_ID.toString(), json.get("queryId").asText());
        assertEquals(open.at("/run/runId").asText(), event.runId());
        assertEquals(event.runId(), json.get("runId").asText());
        assertEquals(open.at("/outputs/0/name").asText(),
                String.join(".", json.at("/output/catalog").asText(), json.at("/output/database").asText(),
                        json.at("/output/table").asText()));
        assertEquals("orders", json.at("/inputs/0/table").asText());
        assertEquals("flag", json.at("/targetColumns/2").asText());
        assertEquals("amount", json.at("/directDependencies/0/outputColumn").asText());
        assertEquals(open.at("/outputs/0/facets/columnLineage/fields/amount/inputFields/0/field").asText(),
                json.at("/directDependencies/0/sources/0/column").asText());
        assertEquals("AGGREGATION", json.at("/columnIndirectDependencies/0/subtype").asText());
        assertEquals("region", json.at("/datasetIndirectDependencies/0/column").asText());
    }

    private static String runId(EventEncoder.ConversionResult result) {
        return result.events().get(0).runId();
    }

    private static LineageInput sample(String queryId, Long start, Long duration) {
        return new LineageInput(
                queryId,
                "warehouse-a",
                SUMMARY,
                List.of(ORDERS, CUSTOMERS),
                List.of(
                        new ColumnEdge("amount", Kind.DIRECT, "TRANSFORMATION", List.of(
                                new ColumnRef(ORDERS, "amt"),
                                new ColumnRef(ORDERS, "qty"))),
                        new ColumnEdge("total", Kind.INDIRECT, "AGGREGATION", List.of(new ColumnRef(ORDERS, "amt")))),
                List.of(new DatasetIndirect(CUSTOMERS, "region", "FILTER")),
                List.of(
                        DatasetSchema.ready(ORDERS, List.of(new SchemaField("amt", "DECIMAL(10,2)"), new SchemaField("qty", "INT"))),
                        DatasetSchema.ready(CUSTOMERS, List.of(new SchemaField("region", "VARCHAR"))),
                        DatasetSchema.ready(SUMMARY, List.of(
                                new SchemaField("amount", "DECIMAL(10,2)"),
                                new SchemaField("total", "DECIMAL(10,2)"),
                                new SchemaField("flag", "TINYINT")))),
                List.of("amount", "total", "flag"),
                start,
                duration,
                2_500L);
    }
}
