package org.apache.doris.plugins.openlineage.convert;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import org.apache.doris.plugins.openlineage.convert.LineageInput.ColumnEdge;
import org.apache.doris.plugins.openlineage.convert.LineageInput.ColumnRef;
import org.apache.doris.plugins.openlineage.convert.LineageInput.DatasetId;
import org.apache.doris.plugins.openlineage.convert.LineageInput.DatasetIndirect;
import org.apache.doris.plugins.openlineage.convert.LineageInput.Kind;

import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

public final class EventConverter {
    public static final String SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json";
    static final URI PRODUCER = URI.create("https://github.com/apache/doris-lineage-plugins/openlineage-plugin");

    private final Supplier<UUID> randomRunIds;

    public EventConverter(Supplier<UUID> randomRunIds) {
        this.randomRunIds = randomRunIds;
    }

    public ConversionResult convert(LineageInput input) {
        List<Diagnostic> diagnostics = new ArrayList<>();
        if (!identifiable(input.output())) {
            diagnostics.add(new Diagnostic(input.queryId(), null, "output dataset identity is missing"));
            return notSent(diagnostics, "receive_time");
        }
        for (DatasetId source : nullToEmpty(input.inputs())) {
            if (!identifiable(source)) {
                diagnostics.add(new Diagnostic(input.queryId(), null, "input dataset identity is missing"));
                return notSent(diagnostics, "receive_time");
            }
        }
        for (DatasetIndirect indirect : nullToEmpty(input.datasetIndirect())) {
            if (indirect == null || !identifiable(indirect.dataset())) {
                diagnostics.add(new Diagnostic(input.queryId(), null, "dataset-level dependency identity is missing"));
                return notSent(diagnostics, "receive_time");
            }
        }

        String namespace = "doris://" + input.cluster();
        String outputName = name(input.output());
        boolean lineageComplete = true;
        OpenLineage ol = new OpenLineage(PRODUCER);
        Map<String, Set<String>> inputColumns = new LinkedHashMap<>();
        Map<String, List<OpenLineage.InputField>> fieldsByColumn = new LinkedHashMap<>();
        for (ColumnEdge edge : nullToEmpty(input.columns())) {
            if (edge == null || edge.kind() == Kind.CONSTANT) {
                continue;
            }
            if (edge.sources() == null || edge.sources().isEmpty()) {
                lineageComplete = false;
                diagnostics.add(new Diagnostic(input.queryId(), edge.outputColumn(), "column source is unresolved"));
                continue;
            }
            List<OpenLineage.InputField> inputFields = fieldsByColumn.computeIfAbsent(edge.outputColumn(), ignored -> new ArrayList<>());
            for (ColumnRef source : edge.sources()) {
                if (source == null || !identifiable(source.dataset()) || blank(source.column())) {
                    lineageComplete = false;
                    diagnostics.add(new Diagnostic(input.queryId(), edge.outputColumn(), "column source is unresolved"));
                    continue;
                }
                inputFields.add(inputField(ol, namespace, source.dataset(), source.column(), edge.kind().name(), edge.subtype()));
                inputColumns.computeIfAbsent(name(source.dataset()), ignored -> new LinkedHashSet<>()).add(source.column());
            }
        }
        OpenLineage.ColumnLineageDatasetFacetFieldsBuilder fields = ol.newColumnLineageDatasetFacetFieldsBuilder();
        boolean anyField = false;
        for (Map.Entry<String, List<OpenLineage.InputField>> entry : fieldsByColumn.entrySet()) {
            if (entry.getValue().isEmpty()) {
                continue;
            }
            fields.put(entry.getKey(), ol.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                    .inputFields(entry.getValue())
                    .build());
            anyField = true;
        }

        List<OpenLineage.InputField> datasetLevel = new ArrayList<>();
        for (DatasetIndirect indirect : nullToEmpty(input.datasetIndirect())) {
            if (indirect == null || blank(indirect.column())) {
                lineageComplete = false;
                diagnostics.add(new Diagnostic(input.queryId(), null, "dataset-level source column is unresolved"));
                continue;
            }
            datasetLevel.add(inputField(ol, namespace, indirect.dataset(), indirect.column(), Kind.INDIRECT.name(), indirect.subtype()));
            inputColumns.computeIfAbsent(name(indirect.dataset()), ignored -> new LinkedHashSet<>()).add(indirect.column());
        }

        Set<String> outputColumns = new LinkedHashSet<>();
        for (Map.Entry<String, List<OpenLineage.InputField>> entry : fieldsByColumn.entrySet()) {
            if (!entry.getValue().isEmpty() && !blank(entry.getKey())) {
                outputColumns.add(entry.getKey());
            }
        }
        OpenLineage.DatasetFacetsBuilder facets = ol.newDatasetFacetsBuilder().schema(schema(ol, outputColumns));
        if (anyField || !datasetLevel.isEmpty()) {
            OpenLineage.ColumnLineageDatasetFacetBuilder columnLineage = ol.newColumnLineageDatasetFacetBuilder()
                    .fields(fields.build());
            if (!datasetLevel.isEmpty()) {
                columnLineage.dataset(datasetLevel);
            }
            facets.columnLineage(columnLineage.build());
        }

        List<OpenLineage.InputDataset> inputs = new ArrayList<>();
        for (DatasetId source : nullToEmpty(input.inputs())) {
            String datasetName = name(source);
            inputs.add(ol.newInputDatasetBuilder()
                    .namespace(namespace)
                    .name(datasetName)
                    .facets(ol.newDatasetFacetsBuilder()
                            .schema(schema(ol, inputColumns.getOrDefault(datasetName, Set.of())))
                            .build())
                    .build());
        }

        OpenLineage.OutputDataset output = ol.newOutputDatasetBuilder()
                .namespace(namespace)
                .name(outputName)
                .facets(facets.build())
                .build();
        OpenLineage.Job job = ol.newJobBuilder().namespace(namespace).name(outputName).build();
        UUID runId = blank(input.queryId())
                ? randomRunIds.get()
                : RunIds.fromQueryId(input.queryId(), namespace, outputName);
        OpenLineage.Run run = ol.newRunBuilder().runId(runId).build();

        boolean dorisTime = input.startTimeMs() != null && input.durationMs() != null && input.durationMs() >= 0;
        ZonedDateTime start = zoned(dorisTime ? input.startTimeMs() : input.receivedAtMs());
        ZonedDateTime complete = zoned(dorisTime ? input.startTimeMs() + input.durationMs() : input.receivedAtMs());
        String startTimeSource = dorisTime ? "doris" : "receive_time";
        if (!dorisTime) {
            diagnostics.add(new Diagnostic(input.queryId(), null, "start_time_source=receive_time"));
        }

        String startEvent = OpenLineageClientUtils.toJson(event(ol, OpenLineage.RunEvent.EventType.START, start, run, job, inputs, output));
        String completeEvent = OpenLineageClientUtils.toJson(event(ol, OpenLineage.RunEvent.EventType.COMPLETE, complete, run, job, inputs, output));
        return new ConversionResult(List.of(startEvent, completeEvent), List.copyOf(diagnostics), lineageComplete, startTimeSource);
    }

    private static OpenLineage.SchemaDatasetFacet schema(OpenLineage ol, Set<String> columns) {
        List<OpenLineage.SchemaDatasetFacetFields> fields = new ArrayList<>();
        for (String column : columns) {
            if (!blank(column)) {
                fields.add(ol.newSchemaDatasetFacetFieldsBuilder().name(column).type("STRING").build());
            }
        }
        return ol.newSchemaDatasetFacetBuilder().fields(fields).build();
    }

    private static OpenLineage.InputField inputField(
            OpenLineage ol,
            String namespace,
            DatasetId dataset,
            String column,
            String type,
            String subtype) {
        OpenLineage.InputFieldTransformationsBuilder transformation = ol.newInputFieldTransformationsBuilder().type(type);
        if (!blank(subtype)) {
            transformation.subtype(subtype);
        }
        return ol.newInputFieldBuilder()
                .namespace(namespace)
                .name(name(dataset))
                .field(column)
                .transformations(List.of(transformation.build()))
                .build();
    }

    private static OpenLineage.RunEvent event(
            OpenLineage ol,
            OpenLineage.RunEvent.EventType type,
            ZonedDateTime time,
            OpenLineage.Run run,
            OpenLineage.Job job,
            List<OpenLineage.InputDataset> inputs,
            OpenLineage.OutputDataset output) {
        return ol.newRunEventBuilder()
                .eventType(type)
                .eventTime(time)
                .run(run)
                .job(job)
                .inputs(inputs)
                .outputs(List.of(output))
                .build();
    }

    private static ConversionResult notSent(List<Diagnostic> diagnostics, String startTimeSource) {
        return new ConversionResult(List.of(), List.copyOf(diagnostics), false, startTimeSource);
    }

    private static boolean identifiable(DatasetId id) {
        return id != null && !blank(id.catalog()) && !blank(id.database()) && !blank(id.table());
    }

    private static String name(DatasetId id) {
        return id.catalog() + "." + id.database() + "." + id.table();
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }

    private static ZonedDateTime zoned(long epochMs) {
        return Instant.ofEpochMilli(epochMs).atZone(ZoneOffset.UTC);
    }

    private static <T> List<T> nullToEmpty(List<T> values) {
        return values == null ? List.of() : values;
    }

    public record Diagnostic(String queryId, String targetColumn, String reason) {
    }

    public record ConversionResult(
            List<String> events,
            List<Diagnostic> diagnostics,
            boolean lineageComplete,
            String startTimeSource) {
    }
}
