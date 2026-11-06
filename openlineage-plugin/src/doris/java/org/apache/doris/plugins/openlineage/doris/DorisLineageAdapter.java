package org.apache.doris.plugins.openlineage.doris;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.lineage.LineageContext;
import org.apache.doris.nereids.lineage.LineageInfo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.plugins.openlineage.convert.DorisEventTimes;
import org.apache.doris.plugins.openlineage.convert.LineageInput;
import org.apache.doris.plugins.openlineage.convert.SourceIdentity;
import org.apache.doris.plugins.openlineage.convert.LineageInput.ColumnEdge;
import org.apache.doris.plugins.openlineage.convert.LineageInput.ColumnRef;
import org.apache.doris.plugins.openlineage.convert.LineageInput.DatasetId;
import org.apache.doris.plugins.openlineage.convert.LineageInput.DatasetIndirect;
import org.apache.doris.plugins.openlineage.convert.LineageInput.Kind;

import java.util.ArrayList;
import java.util.List;

/**
 * The only Doris import boundary. It copies already-analyzed lineage into {@link LineageInput}.
 */
public final class DorisLineageAdapter {
    private DorisLineageAdapter() {
    }

    public static LineageInput toInput(LineageInfo info, String cluster, long receivedAtMs) {
        LineageContext context = info.getContext();
        DatasetId output = datasetId(info.getTargetTable());
        List<DatasetId> inputs = new ArrayList<>();
        if (info.getTableLineageSet() != null) {
            for (TableIf table : info.getTableLineageSet()) {
                // Null marks a source Doris named but whose catalog/database could not be resolved.
                // A table that is both read and written stays in inputs.
                inputs.add(datasetId(table));
            }
        }
        List<ColumnEdge> columns = new ArrayList<>();
        addDirect(info, columns);
        addIndirect(info, columns);
        List<DatasetIndirect> datasetIndirect = datasetIndirect(info);
        return new LineageInput(
                context == null ? null : context.getQueryId(),
                cluster,
                output,
                inputs,
                columns,
                datasetIndirect,
                eventTimes(context).startMs(),
                eventTimes(context).durationMs(),
                receivedAtMs);
    }

    private static void addDirect(LineageInfo info, List<ColumnEdge> columns) {
        if (info.getDirectLineageMap() == null) {
            return;
        }
        info.getDirectLineageMap().forEach((output, byType) -> {
            if (byType == null) {
                return;
            }
            byType.asMap().forEach((type, expressions) -> addEdge(columns, edge(output, Kind.DIRECT, type == null ? null : type.name(), expressions)));
        });
    }

    private static void addIndirect(LineageInfo info, List<ColumnEdge> columns) {
        if (info.getOutputIndirectLineageMap() == null) {
            return;
        }
        info.getOutputIndirectLineageMap().forEach((output, byType) -> {
            if (byType == null) {
                return;
            }
            byType.asMap().forEach((type, expressions) -> addEdge(columns, edge(output, Kind.INDIRECT, type == null ? null : type.name(), expressions)));
        });
    }

    private static void addEdge(List<ColumnEdge> columns, ColumnEdge edge) {
        if (edge != null) {
            columns.add(edge);
        }
    }

    private static List<ColumnRef> sourceRefs(Expression expression) {
        List<ColumnRef> sources = new ArrayList<>();
        if (expression == null || expression.getInputSlots() == null) {
            return sources;
        }
        for (Slot slot : expression.getInputSlots()) {
            sources.add(columnRef(slot));
        }
        return sources;
    }

    private static ColumnEdge edge(SlotReference output, Kind kind, String subtype, Iterable<Expression> expressions) {
        List<ColumnRef> sources = new ArrayList<>();
        if (expressions != null) {
            for (Expression expression : expressions) {
                sources.addAll(sourceRefs(expression));
            }
        }
        if (sources.isEmpty()) {
            // Doris records literal projections as TRANSFORMATION with no input slots. That is a constant, not a failure.
            return null;
        }
        return new ColumnEdge(columnName(output), kind, subtype, sources);
    }

    private static List<DatasetIndirect> datasetIndirect(LineageInfo info) {
        List<DatasetIndirect> indirect = new ArrayList<>();
        if (info.getDatasetIndirectLineageMap() == null) {
            return indirect;
        }
        info.getDatasetIndirectLineageMap().forEach((type, expression) -> {
            String subtype = type == null ? null : type.name();
            List<ColumnRef> sources = sourceRefs(expression);
            if (sources.isEmpty()) {
                return;
            }
            for (ColumnRef source : sources) {
                indirect.add(new DatasetIndirect(source.dataset(), source.column(), subtype));
            }
        });
        return indirect;
    }

    private static ColumnRef columnRef(Slot slot) {
        if (!(slot instanceof SlotReference reference)) {
            return new ColumnRef(null, columnName(slot));
        }
        SourceIdentity.Choice choice = SourceIdentity.choose(
                qualifiers(reference.getOriginalTable().orElse(null)),
                columnOf(reference.getOriginalColumn().orElse(null)),
                qualifiers(reference.getOneLevelTable().orElse(null)),
                columnName(slot));
        DatasetId dataset = choice.catalog() == null ? null : new DatasetId(choice.catalog(), choice.database(), choice.table());
        return new ColumnRef(dataset, choice.column());
    }

    private static SourceIdentity.Qualifiers qualifiers(TableIf table) {
        DatasetId id = datasetId(table);
        return id == null ? null : new SourceIdentity.Qualifiers(id.catalog(), id.database(), id.table());
    }

    private static String columnOf(Column column) {
        if (column == null) {
            return null;
        }
        try {
            return column.getName();
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static DatasetId datasetId(TableIf table) {
        if (table == null) {
            return null;
        }
        try {
            List<String> parts = table.getFullQualifiers();
            if (parts == null || parts.size() < 3 || blank(parts.get(0)) || blank(parts.get(1)) || blank(parts.get(2))) {
                return null;
            }
            if ("null".equals(parts.get(0)) || "null".equals(parts.get(1))) {
                return null;
            }
            return new DatasetId(parts.get(0), parts.get(1), parts.get(2));
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static String columnName(Slot slot) {
        if (slot == null) {
            return null;
        }
        try {
            String name = slot.getName();
            return blank(name) ? null : name;
        } catch (Exception e) {
            return null;
        }
    }

    private static DorisEventTimes.Window eventTimes(LineageContext context) {
        if (context == null) {
            return new DorisEventTimes.Window(null, null);
        }
        return DorisEventTimes.from(context.getTimestampMs(), context.getDurationMs());
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }
}
