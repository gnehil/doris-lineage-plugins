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
import java.util.List;
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
        List<OpenLineage.InputDataset> inputs = new ArrayList<>();
        for (DatasetId source : nullToEmpty(input.inputs())) {
            inputs.add(ol.newInputDatasetBuilder().namespace(namespace).name(name(source)).build());
        }

        OpenLineage.ColumnLineageDatasetFacetFieldsBuilder fields =
                ol.newColumnLineageDatasetFacetFieldsBuilder();
        boolean anyField = false;
        for (ColumnEdge edge : nullToEmpty(input.columns())) {
            if (edge == null || edge.kind() == Kind.CONSTANT) {
                continue;
            }
            List<OpenLineage.InputField> inputFields = new ArrayList<>();
            for (ColumnRef source : nullToEmpty(edge.sources())) {
                if (source == null || !identifiable(source.dataset()) || blank(source.column())) {
                    lineageComplete = false;
                    diagnostics.add(new Diagnostic(input.queryId(), edge.outputColumn(), "column source is unresolved"));
                    continue;
                }
                OpenLineage.InputFieldTransformationsBuilder transformation =
                        ol.newInputFieldTransformationsBuilder().type(edge.kind().name());
                if (!blank(edge.subtype())) {
                    transformation.subtype(edge.subtype());
                }
                OpenLineage.InputFieldBuilder field = ol.newInputFieldBuilder()
                        .namespace(namespace)
                        .name(name(source.dataset()))
                        .field(source.column())
                        .transformations(List.of(transformation.build()));
                inputFields.add(field.build());
            }
            if (inputFields.isEmpty()) {
                continue;
            }
            fields.put(edge.outputColumn(), ol.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                    .inputFields(inputFields)
                    .build());
            anyField = true;
        }

        List<OpenLineage.InputField> datasetLevel = new ArrayList<>();
        for (DatasetIndirect indirect : nullToEmpty(input.datasetIndirect())) {
            OpenLineage.InputFieldTransformationsBuilder transformation =
                    ol.newInputFieldTransformationsBuilder().type(Kind.INDIRECT.name());
            if (!blank(indirect.subtype())) {
                transformation.subtype(indirect.subtype());
            }
            datasetLevel.add(ol.newInputFieldBuilder()
                    .namespace(namespace)
                    .name(name(indirect.dataset()))
                    .transformations(List.of(transformation.build()))
                    .build());
        }

        OpenLineage.DatasetFacetsBuilder facets = ol.newDatasetFacetsBuilder();
        if (anyField || !datasetLevel.isEmpty()) {
            OpenLineage.ColumnLineageDatasetFacetBuilder columnLineage = ol.newColumnLineageDatasetFacetBuilder();
            if (anyField) {
                columnLineage.fields(fields.build());
            }
            if (!datasetLevel.isEmpty()) {
                columnLineage.dataset(datasetLevel);
            }
            facets.columnLineage(columnLineage.build());
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
