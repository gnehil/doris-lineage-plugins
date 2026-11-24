package org.apache.doris.plugins.openlineage.convert;

import java.util.List;

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

    public record DatasetId(String catalog, String database, String table) {
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
