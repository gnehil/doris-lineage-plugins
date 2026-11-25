package org.apache.doris.plugins.openlineage.convert;

import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Doris-free lineage the adapter will fill. The converter does not parse SQL.
 */
public record LineageInput(
        String queryId,
        String cluster,
        DatasetId output,
        List<DatasetId> inputs,
        List<ColumnEdge> columns,
        List<DatasetIndirect> datasetIndirect,
        List<DatasetSchema> schemas,
        List<String> outputColumns,
        Long startTimeMs,
        Long durationMs,
        long receivedAtMs) {

    String namespace() {
        return "doris://" + cluster;
    }

    UUID runId(Supplier<UUID> randomRunIds) {
        return blank(queryId) ? randomRunIds.get()
                : RunIds.fromQueryId(queryId, namespace(), output.qualifiedName());
    }

    String datasetProblem() {
        if (!DatasetId.identifiable(output)) {
            return "output dataset identity is missing";
        }
        if (inputs != null && inputs.stream().anyMatch(id -> !DatasetId.identifiable(id))) {
            return "input dataset identity is missing";
        }
        if (datasetIndirect != null && datasetIndirect.stream()
                .anyMatch(edge -> edge == null || !DatasetId.identifiable(edge.dataset()))) {
            return "dataset-level dependency identity is missing";
        }
        return null;
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }

    public record DatasetId(String catalog, String database, String table) {
        static boolean identifiable(DatasetId id) {
            return id != null && !blank(id.catalog) && !blank(id.database) && !blank(id.table);
        }

        public String qualifiedName() {
            return catalog + "." + database + "." + table;
        }
    }

    public record ColumnRef(DatasetId dataset, String column) {
    }

    public enum Kind {
        DIRECT,
        INDIRECT
    }

    public record ColumnEdge(String outputColumn, Kind kind, String subtype, List<ColumnRef> sources) {
    }

    public record DatasetIndirect(DatasetId dataset, String column, String subtype) {
    }

    public record SchemaField(String name, String type) {
    }

    public record DatasetSchema(DatasetId dataset, List<SchemaField> fields, boolean available) {
        public static DatasetSchema ready(DatasetId dataset, List<SchemaField> fields) {
            return new DatasetSchema(dataset, fields, true);
        }

        public static DatasetSchema unavailable(DatasetId dataset) {
            return new DatasetSchema(dataset, List.of(), false);
        }
    }
}
