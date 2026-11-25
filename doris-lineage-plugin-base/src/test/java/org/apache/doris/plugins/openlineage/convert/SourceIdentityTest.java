package org.apache.doris.plugins.openlineage.convert;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class SourceIdentityTest {
    @Test
    void viewColumnUsesTheUnderlyingTableAndOriginalColumn() {
        SourceIdentity.Qualifiers view = new SourceIdentity.Qualifiers("internal", "sales", "orders_view");
        SourceIdentity.Qualifiers base = new SourceIdentity.Qualifiers("internal", "sales", "orders");

        SourceIdentity.Choice choice = SourceIdentity.choose(base, "amount", view, "amt");

        assertEquals("internal", choice.catalog());
        assertEquals("sales", choice.database());
        assertEquals("orders", choice.table());
        assertEquals("amount", choice.column());
    }

    @Test
    void fallsBackToTheOneLevelTableWhenTheOriginalTableIsMissing() {
        SourceIdentity.Qualifiers view = new SourceIdentity.Qualifiers("internal", "sales", "orders_view");

        SourceIdentity.Choice choice = SourceIdentity.choose(null, null, view, "amt");

        assertEquals("orders_view", choice.table());
        assertEquals("amt", choice.column());
    }

    @Test
    void doesNotInventAnIdentityForAnUnusableTable() {
        SourceIdentity.Choice choice = SourceIdentity.choose(
                new SourceIdentity.Qualifiers("null", "sales", "orders"),
                "amount",
                null,
                "amt");

        assertNull(choice.catalog());
        assertEquals("amount", choice.column());
    }
}
