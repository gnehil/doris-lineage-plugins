package org.apache.doris.plugins.openlineage.convert;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ColumnTypesTest {
    private static final String SQL = "STRUCT<id:INT>";
    private static final String DEBUG = "STRUCT<StructField ( name=id, dataType=INT, nullable=true )>";

    @Test
    void externalStructUsesTheOriginalColumnSqlType() {
        assertEquals(SQL, ColumnTypes.sqlType("id", "id", SQL, "id", DEBUG));
    }

    @Test
    void fallsBackToTheOneLevelSqlTypeWhenThatNameWasChosen() {
        assertEquals(SQL, ColumnTypes.sqlType("view_id", "id", null, "view_id", SQL));
    }

    @Test
    void omitsUnknownTypesInsteadOfUsingTheDebugString() {
        assertNull(ColumnTypes.sqlType("id", null, null, null, DEBUG));
        assertNull(ColumnTypes.sqlType("id", "other", "  ", "view_id", DEBUG));
    }
}
