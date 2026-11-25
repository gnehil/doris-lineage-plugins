package org.apache.doris.plugins.openlineage.convert;

/**
 * Chooses the physical source behind a column reference. Doris 4.0.6 can leave a view in the
 * one-level table while {@code tableLineageSet} names the underlying table. Prefer the original
 * table and original column so column lineage matches inputs.
 */
public final class SourceIdentity {
    private SourceIdentity() {
    }

    public record Qualifiers(String catalog, String database, String table) {
    }

    public record Choice(String catalog, String database, String table, String column) {
    }

    public static Choice choose(Qualifiers originalTable, String originalColumn, Qualifiers oneLevelTable, String slotName) {
        Qualifiers table = usable(originalTable) ? originalTable : usable(oneLevelTable) ? oneLevelTable : null;
        String column = blank(originalColumn) ? slotName : originalColumn;
        if (table == null) {
            return new Choice(null, null, null, blank(column) ? null : column);
        }
        return new Choice(table.catalog(), table.database(), table.table(), blank(column) ? null : column);
    }

    private static boolean usable(Qualifiers table) {
        return table != null
                && !blank(table.catalog())
                && !blank(table.database())
                && !blank(table.table())
                && !"null".equals(table.catalog())
                && !"null".equals(table.database());
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }
}
