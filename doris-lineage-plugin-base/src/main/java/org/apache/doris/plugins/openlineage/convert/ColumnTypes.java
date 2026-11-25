package org.apache.doris.plugins.openlineage.convert;

/**
 * Schema types must be SQL text from the same column identity used for the field name.
 * Nereids {@code DataType.toString()} is a debug form and is never accepted.
 */
public final class ColumnTypes {
    private ColumnTypes() {
    }

    public static String sqlType(
            String chosenName,
            String originalName,
            String originalSql,
            String oneLevelName,
            String oneLevelSql) {
        if (matches(chosenName, originalName) && !blank(originalSql)) {
            return originalSql;
        }
        if (matches(chosenName, oneLevelName) && !blank(oneLevelSql)) {
            return oneLevelSql;
        }
        return null;
    }

    private static boolean matches(String chosen, String candidate) {
        return chosen != null && chosen.equals(candidate);
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }
}
