package org.apache.doris.plugins.openlineage.convert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.plugins.openlineage.convert.LineageInput.ColumnEdge;
import org.apache.doris.plugins.openlineage.convert.LineageInput.ColumnRef;
import org.apache.doris.plugins.openlineage.convert.LineageInput.DatasetId;
import org.apache.doris.plugins.openlineage.convert.LineageInput.DatasetIndirect;
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
        EventConverter.ConversionResult result = new EventConverter(UUID::randomUUID).convert(sample(QUERY_ID.toString(), 1_000L, 30L));

        assertTrue(result.lineageComplete());
        assertEquals("doris", result.startTimeSource());
        assertEquals(2, result.events().size());
        JsonNode start = JSON.readTree(result.events().get(0));
        JsonNode complete = JSON.readTree(result.events().get(1));
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
        assertTrue(start.at("/outputs/0/facets/columnLineage/fields/literal_only").isMissingNode());
        JsonNode datasetLevel = start.at("/outputs/0/facets/columnLineage/dataset");
        assertEquals("internal.sales.customers", datasetLevel.get(0).get("name").asText());
        assertEquals("FILTER", datasetLevel.get(0).at("/transformations/0/subtype").asText());
        assertTrue(datasetLevel.get(0).path("field").isMissingNode() || datasetLevel.get(0).path("field").isNull());
    }

    @Test
    void derivesAStableRunIdWhenTheQueryIdIsNotAUuid() {
        EventConverter converter = new EventConverter(UUID::randomUUID);
        LineageInput input = sample("query-17", 1_000L, 0L);
        EventConverter.ConversionResult once = converter.convert(input);
        EventConverter.ConversionResult twice = converter.convert(input);
        assertEquals(runId(once), runId(twice));
        assertFalse(runId(once).equals(QUERY_ID.toString()));
        UUID expected = RunIds.version5(RunIds.NAMESPACE, RunIds.nameBytes("doris://warehouse-a", "hive.dw.order_summary", "query-17"));
        assertEquals(expected.toString(), runId(once));
        assertEquals(runId(once), runId(new EventConverter(UUID::randomUUID).convert(input)));
    }

    @Test
    void usesOneRandomRunIdWhenTheQueryIdIsMissing() throws Exception {
        UUID random = UUID.fromString("22222222-2222-2222-2222-222222222222");
        EventConverter.ConversionResult result = new EventConverter(() -> random).convert(sample("  ", null, null));
        JsonNode start = JSON.readTree(result.events().get(0));
        JsonNode complete = JSON.readTree(result.events().get(1));
        assertEquals(random.toString(), start.get("run").get("runId").asText());
        assertEquals(random.toString(), complete.get("run").get("runId").asText());
        assertEquals("receive_time", result.startTimeSource());
        assertEquals(start.get("eventTime").asText(), complete.get("eventTime").asText());
    }

    @Test
    void fallsBackWhenDurationIsNegative() {
        EventConverter.ConversionResult result = new EventConverter(UUID::randomUUID).convert(sample(QUERY_ID.toString(), 5_000L, -1L));
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
                1_000L,
                1L,
                9_000L);
        EventConverter.ConversionResult result = new EventConverter(UUID::randomUUID).convert(input);
        assertFalse(result.lineageComplete());
        assertEquals("amount", result.diagnostics().get(0).targetColumn());
        JsonNode start = JSON.readTree(result.events().get(0));
        assertTrue(start.at("/outputs/0/facets/columnLineage/fields/amount").isMissingNode());
        assertFalse(start.toString().contains("\"field\":\"amt\""));
    }

    @Test
    void doesNotSendWhenTheOutputIdentityIsMissing() {
        LineageInput input = sample(QUERY_ID.toString(), 1_000L, 1L);
        LineageInput broken = new LineageInput(
                input.queryId(), input.cluster(), new DatasetId("hive", " ", "order_summary"),
                input.inputs(), input.columns(), input.datasetIndirect(),
                input.startTimeMs(), input.durationMs(), input.receivedAtMs());
        EventConverter.ConversionResult result = new EventConverter(UUID::randomUUID).convert(broken);
        assertTrue(result.events().isEmpty());
        assertFalse(result.lineageComplete());
    }

    private static String runId(EventConverter.ConversionResult result) {
        int at = result.events().get(0).indexOf("\"runId\":\"");
        return result.events().get(0).substring(at + 9, at + 9 + 36);
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
                        new ColumnEdge("total", Kind.INDIRECT, "AGGREGATION", List.of(new ColumnRef(ORDERS, "amt"))),
                        new ColumnEdge("literal_only", Kind.CONSTANT, null, List.of())),
                List.of(new DatasetIndirect(CUSTOMERS, "FILTER")),
                start,
                duration,
                2_500L);
    }
}
