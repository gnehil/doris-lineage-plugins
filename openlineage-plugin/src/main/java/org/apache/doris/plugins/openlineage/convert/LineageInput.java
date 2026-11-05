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
        Long startTimeMs,
        Long durationMs,
        long receivedAtMs) {

    public record DatasetId(String catalog, String database, String table) {
    }

    public record ColumnRef(DatasetId dataset, String column) {
    }

    public enum Kind {
        DIRECT,
        INDIRECT,
        CONSTANT
    }

    public record ColumnEdge(String outputColumn, Kind kind, String subtype, List<ColumnRef> sources) {
    }

    public record DatasetIndirect(DatasetId dataset, String subtype) {
    }
}
