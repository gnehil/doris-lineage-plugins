package org.apache.doris.plugins.openlineage.convert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.doris.plugins.openlineage.convert.LineageInput.ColumnEdge;
import org.apache.doris.plugins.openlineage.convert.LineageInput.ColumnRef;
import org.apache.doris.plugins.openlineage.convert.LineageInput.DatasetId;
import org.apache.doris.plugins.openlineage.convert.LineageInput.DatasetIndirect;
import org.apache.doris.plugins.openlineage.convert.LineageInput.Kind;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

public final class DorisJsonEncoder implements EventEncoder {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final Supplier<UUID> randomRunIds;

    public DorisJsonEncoder(Supplier<UUID> randomRunIds) {
        this.randomRunIds = randomRunIds;
    }

    @Override
    public boolean requiresSchemas() {
        return false;
    }

    @Override
    public ConversionResult convert(LineageInput input) {
        String problem = input.datasetProblem();
        if (problem != null) {
            return new ConversionResult(List.of(), List.of(new Diagnostic(input.queryId(), null, problem)), false, null);
        }

        List<Diagnostic> diagnostics = new ArrayList<>();
        List<Dependency> direct = new ArrayList<>();
        List<Dependency> columnIndirect = new ArrayList<>();
        for (ColumnEdge edge : orEmpty(input.columns())) {
            if (edge == null) {
                continue;
            }
            List<ColumnRef> reported = orEmpty(edge.sources());
            List<ColumnRef> sources = reported.stream().filter(DorisJsonEncoder::validSource).toList();
            if (sources.isEmpty() || sources.size() != reported.size() || blank(edge.outputColumn())) {
                diagnostics.add(new Diagnostic(input.queryId(), edge.outputColumn(), "column source or target is unresolved"));
            }
            if (!sources.isEmpty() && !blank(edge.outputColumn())) {
                List<Dependency> dependencies = edge.kind() == Kind.DIRECT ? direct : columnIndirect;
                dependencies.add(new Dependency(edge.outputColumn(), edge.subtype(), sources));
            }
        }

        List<DatasetIndirect> datasetIndirect = new ArrayList<>();
        for (DatasetIndirect edge : orEmpty(input.datasetIndirect())) {
            if (blank(edge.column())) {
                diagnostics.add(new Diagnostic(input.queryId(), null, "dataset-level source column is unresolved"));
            } else {
                datasetIndirect.add(edge);
            }
        }

        String runId = input.runId(randomRunIds).toString();
        Payload payload = new Payload(1, input.queryId(), runId, input.cluster(), orEmpty(input.inputs()),
                input.output(), orEmpty(input.outputColumns()), direct, columnIndirect, datasetIndirect);
        try {
            return new ConversionResult(List.of(new Event("LINEAGE", runId, JSON.writeValueAsBytes(payload))),
                    List.copyOf(diagnostics), diagnostics.isEmpty(), null);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("could not encode Doris lineage JSON", e);
        }
    }

    private static boolean validSource(ColumnRef ref) {
        return ref != null && DatasetId.identifiable(ref.dataset()) && !blank(ref.column());
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }

    private static <T> List<T> orEmpty(List<T> list) {
        return list == null ? List.of() : list;
    }

    private record Dependency(String outputColumn, String subtype, List<ColumnRef> sources) {
    }

    private record Payload(int schemaVersion, String queryId, String runId, String cluster,
                           List<DatasetId> inputs, DatasetId output, List<String> targetColumns,
                           List<Dependency> directDependencies, List<Dependency> columnIndirectDependencies,
                           List<DatasetIndirect> datasetIndirectDependencies) {
    }
}
