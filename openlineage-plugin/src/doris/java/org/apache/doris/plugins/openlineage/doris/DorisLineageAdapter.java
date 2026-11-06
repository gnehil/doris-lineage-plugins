package org.apache.doris.plugins.openlineage.doris;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.lineage.LineageContext;
import org.apache.doris.nereids.lineage.LineageInfo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.plugins.openlineage.convert.LineageInput;
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
                DatasetId id = datasetId(table);
                if (id != null && !id.equals(output)) {
                    inputs.add(id);
                }
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
                startTime(context),
                duration(context),
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
            byType.asMap().forEach((type, expressions) ->
                    columns.add(edge(output, Kind.DIRECT, type == null ? null : type.name(), expressions)));
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
            byType.asMap().forEach((type, expressions) ->
                    columns.add(edge(output, Kind.INDIRECT, type == null ? null : type.name(), expressions)));
        });
    }

    private static ColumnEdge edge(SlotReference output, Kind kind, String subtype, Iterable<Expression> expressions) {
        List<ColumnRef> sources = new ArrayList<>();
        if (expressions != null) {
            for (Expression expression : expressions) {
                if (expression == null || expression.getInputSlots() == null) {
                    continue;
                }
                for (Slot slot : expression.getInputSlots()) {
                    sources.add(new ColumnRef(datasetId(tableOf(slot)), columnName(slot)));
                }
            }
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
            if (expression == null || expression.getInputSlots() == null || expression.getInputSlots().isEmpty()) {
                indirect.add(new DatasetIndirect(null, null, subtype));
                return;
            }
            for (Slot slot : expression.getInputSlots()) {
                indirect.add(new DatasetIndirect(datasetId(tableOf(slot)), columnName(slot), subtype));
            }
        });
        return indirect;
    }

    private static TableIf tableOf(Slot slot) {
        if (!(slot instanceof SlotReference reference)) {
            return null;
        }
        if (reference.getOneLevelTable().isPresent()) {
            return reference.getOneLevelTable().get();
        }
        return reference.getOriginalTable().orElse(null);
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

    private static Long startTime(LineageContext context) {
        if (context == null || context.getTimestampMs() == LineageContext.UNKNOWN_TIMESTAMP_MS) {
            return null;
        }
        return context.getTimestampMs();
    }

    private static Long duration(LineageContext context) {
        if (context == null || context.getDurationMs() == LineageContext.UNKNOWN_DURATION_MS) {
            return null;
        }
        return context.getDurationMs();
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }
}
