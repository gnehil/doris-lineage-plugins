package org.apache.doris.plugins.openlineage.doris;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.lineage.LineageInfo;
import org.apache.doris.plugins.openlineage.config.EventFormat;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DorisLineageAdapterTest {
    @Test
    void skipsSchemaTraversalForDorisJsonAndRetainsItForOpenLineage() {
        var nativeInfo = new CountingLineageInfo();
        var nativeInput = DorisLineageAdapter.toInput(nativeInfo, "warehouse", 0,
                EventFormat.DORIS_JSON.encoder().requiresSchemas());
        assertTrue(nativeInput.schemas().isEmpty());
        assertEquals(1, nativeInfo.targetTableReads);

        var openLineageInfo = new CountingLineageInfo();
        DorisLineageAdapter.toInput(openLineageInfo, "warehouse", 0,
                EventFormat.OPENLINEAGE.encoder().requiresSchemas());
        assertEquals(2, openLineageInfo.targetTableReads);
    }

    private static final class CountingLineageInfo extends LineageInfo {
        private int targetTableReads;

        @Override
        public TableIf getTargetTable() {
            targetTableReads++;
            return super.getTargetTable();
        }
    }
}
