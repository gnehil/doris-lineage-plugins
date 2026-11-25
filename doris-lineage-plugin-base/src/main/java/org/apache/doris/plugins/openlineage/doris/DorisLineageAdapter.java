package org.apache.doris.plugins.openlineage.doris;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.lineage.LineageContext;
import org.apache.doris.nereids.lineage.LineageInfo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.plugins.openlineage.convert.ColumnTypes;
import org.apache.doris.plugins.openlineage.convert.DorisEventTimes;
import org.apache.doris.plugins.openlineage.convert.LineageInput;
import org.apache.doris.plugins.openlineage.convert.SourceIdentity;
import org.apache.doris.plugins.openlineage.convert.LineageInput.ColumnEdge;
import org.apache.doris.plugins.openlineage.convert.LineageInput.ColumnRef;
import org.apache.doris.plugins.openlineage.convert.LineageInput.DatasetId;
import org.apache.doris.plugins.openlineage.convert.LineageInput.DatasetIndirect;
import org.apache.doris.plugins.openlineage.convert.LineageInput.Kind;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Copies already-analyzed Doris lineage into {@link LineageInput}.
 */
public final class DorisLineageAdapter {
    private DorisLineageAdapter() {
    }

    public static LineageInput toInput(LineageInfo info, String cluster, long receivedAtMs, boolean includeSchemas) {
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
        DorisEventTimes.Window times = eventTimes(context);
        return new LineageInput(
                context == null ? null : context.getQueryId(),
                cluster,
                output,
                inputs,
                columns,
                datasetIndirect,
                includeSchemas ? schemas(info) : List.of(),
                outputColumns(info),
                times.startMs(),
                times.durationMs(),
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

    private static List<LineageInput.DatasetSchema> schemas(LineageInfo info) {
        List<LineageInput.DatasetSchema> schemas = new ArrayList<>();
        addSchema(schemas, info, info.getTargetTable());
        if (info.getTableLineageSet() != null) {
            for (TableIf table : info.getTableLineageSet()) {
                addSchema(schemas, info, table);
            }
        }
        return schemas;
    }

    private static void addSchema(List<LineageInput.DatasetSchema> schemas, LineageInfo info, TableIf table) {
        DatasetId id = datasetId(table);
        if (id == null) {
            return;
        }
        if (table instanceof ExternalTable) {
            List<LineageInput.SchemaField> known = knownFields(info, id);
            schemas.add(known.isEmpty()
                    ? LineageInput.DatasetSchema.unavailable(id)
                    : LineageInput.DatasetSchema.ready(id, known));
            return;
        }
        List<LineageInput.SchemaField> fields = new ArrayList<>();
        List<Column> columns;
        try {
            columns = table.getBaseSchema(false);
        } catch (RuntimeException e) {
            schemas.add(LineageInput.DatasetSchema.unavailable(id));
            return;
        }
        if (columns == null) {
            schemas.add(LineageInput.DatasetSchema.unavailable(id));
            return;
        }
        if (columns != null) {
            for (org.apache.doris.catalog.Column column : columns) {
                if (column == null || blank(column.getName())) {
                    continue;
                }
                String type = null;
                try {
                    if (column.getType() != null) {
                        type = column.getType().toSql();
                    }
                } catch (RuntimeException ignored) {
                    type = null;
                }
                fields.add(new LineageInput.SchemaField(column.getName(), type));
            }
        }
        if (fields.isEmpty()) {
            schemas.add(LineageInput.DatasetSchema.unavailable(id));
            return;
        }
        schemas.add(LineageInput.DatasetSchema.ready(id, fields));
    }

    private static List<LineageInput.SchemaField> knownFields(LineageInfo info, DatasetId dataset) {
        Map<String, String> fields = new LinkedHashMap<>();
        if (info.getTargetColumns() != null && dataset.equals(datasetId(info.getTargetTable()))) {
            for (Slot slot : info.getTargetColumns()) {
                rememberOutput(fields, slot);
            }
        }
        rememberExpressions(fields, dataset, info.getDirectLineageMap());
        rememberExpressions(fields, dataset, info.getOutputIndirectLineageMap());
        if (info.getDatasetIndirectLineageMap() != null) {
            for (Expression expression : info.getDatasetIndirectLineageMap().values()) {
                rememberExpression(fields, dataset, expression);
            }
        }
        List<LineageInput.SchemaField> result = new ArrayList<>();
        fields.forEach((name, type) -> result.add(new LineageInput.SchemaField(name, type)));
        return result;
    }

    private static void rememberExpressions(
            Map<String, String> fields,
            DatasetId dataset,
            Map<SlotReference, ? extends com.google.common.collect.SetMultimap<?, Expression>> lineage) {
        if (lineage == null) {
            return;
        }
        for (com.google.common.collect.SetMultimap<?, Expression> byType : lineage.values()) {
            if (byType == null) {
                continue;
            }
            for (Expression expression : byType.values()) {
                rememberExpression(fields, dataset, expression);
            }
        }
    }

    private static void rememberExpression(Map<String, String> fields, DatasetId dataset, Expression expression) {
        if (expression == null || expression.getInputSlots() == null) {
            return;
        }
        for (Slot slot : expression.getInputSlots()) {
            ColumnRef ref = columnRef(slot);
            if (ref.dataset() != null && dataset.equals(ref.dataset())) {
                rememberNamed(fields, ref.column(), typeOf(slot));
            }
        }
    }

    private static void rememberOutput(Map<String, String> fields, Slot slot) {
        rememberNamed(fields, columnName(slot), typeOf(slot));
    }

    private static void rememberNamed(Map<String, String> fields, String name, String type) {
        if (blank(name) || fields.containsKey(name)) {
            return;
        }
        fields.put(name, type);
    }

    private static String typeOf(Slot slot) {
        if (!(slot instanceof SlotReference reference)) {
            return null;
        }
        Column original = reference.getOriginalColumn().orElse(null);
        Column oneLevel = reference.getOneLevelColumn().orElse(null);
        return ColumnTypes.sqlType(columnRef(slot).column(), columnOf(original), sqlType(original), columnOf(oneLevel), sqlType(oneLevel));
    }

    private static String sqlType(Column column) {
        if (column == null || column.getType() == null) {
            return null;
        }
        try {
            String sql = column.getType().toSql();
            return blank(sql) ? null : sql;
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static List<String> outputColumns(LineageInfo info) {
        List<String> columns = new ArrayList<>();
        if (info.getTargetColumns() == null) {
            return columns;
        }
        for (Slot slot : info.getTargetColumns()) {
            String name = columnName(slot);
            if (!blank(name)) {
                columns.add(name);
            }
        }
        return columns;
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
