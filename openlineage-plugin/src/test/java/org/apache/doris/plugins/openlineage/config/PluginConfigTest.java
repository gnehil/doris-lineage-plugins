package org.apache.doris.plugins.openlineage.config;

import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PluginConfigTest {
    @Test
    void acceptsTheFourFieldsAndDoesNotAppendAPath() throws Exception {
        PluginConfig config = PluginConfig.parse(new StringReader("""
                url: http://marquez:5000/api/v1/lineage
                cluster: warehouse-a
                auth:
                  type: bearer
                  token: secret
                """));

        assertEquals("http://marquez:5000/api/v1/lineage", config.url().toString());
        assertEquals(5000, config.timeoutMs());
        assertEquals("warehouse-a", config.cluster());
        assertEquals("doris://warehouse-a", config.namespace());
        assertTrue(config.auth().bearer());
        assertEquals("secret", config.auth().token());
    }

    @Test
    void rejectsUnknownFields() {
        PluginConfig.ConfigException error = assertThrows(PluginConfig.ConfigException.class,
                () -> PluginConfig.parse(new StringReader("""
                        url: https://marquez.example/api/v1/lineage
                        cluster: warehouse-a
                        transport: kafka
                        auth:
                          type: none
                        """)));
        assertTrue(error.getMessage().contains("unknown"));
    }

    @Test
    void rejectsAuthTypesOtherThanNoneOrBearer() {
        assertThrows(PluginConfig.ConfigException.class, () -> PluginConfig.parse(new StringReader("""
                url: https://marquez.example/api/v1/lineage
                cluster: warehouse-a
                auth:
                  type: api_key
                  token: secret
                """)));
    }

    @Test
    void rejectsASecondYamlDocument() {
        assertThrows(PluginConfig.ConfigException.class, () -> PluginConfig.parse(new StringReader("""
                url: https://marquez.example/api/v1/lineage
                cluster: warehouse-a
                auth:
                  type: none
                ---
                transport: kafka
                """)));
    }

    @Test
    void rejectsAnExplicitNullTimeout() {
        assertThrows(PluginConfig.ConfigException.class, () -> PluginConfig.parse(new StringReader("""
                url: https://marquez.example/api/v1/lineage
                timeoutMs: null
                cluster: warehouse-a
                auth:
                  type: none
                """)));
        assertThrows(PluginConfig.ConfigException.class, () -> PluginConfig.parse(new StringReader("""
                url: https://marquez.example/api/v1/lineage
                timeoutMs: ~
                cluster: warehouse-a
                auth:
                  type: none
                """)));
    }

    @Test
    void rejectsABlankTimeout() {
        assertThrows(PluginConfig.ConfigException.class, () -> PluginConfig.parse(new StringReader("""
                url: https://marquez.example/api/v1/lineage
                timeoutMs: ""
                cluster: warehouse-a
                auth:
                  type: none
                """)));
        assertThrows(PluginConfig.ConfigException.class, () -> PluginConfig.parse(new StringReader("""
                url: https://marquez.example/api/v1/lineage
                timeoutMs: "   "
                cluster: warehouse-a
                auth:
                  type: none
                """)));
    }

    @Test
    void rejectsAFractionalTimeout() {
        assertThrows(PluginConfig.ConfigException.class, () -> PluginConfig.parse(new StringReader("""
                url: https://marquez.example/api/v1/lineage
                timeoutMs: 1.9
                cluster: warehouse-a
                auth:
                  type: none
                """)));
    }

    @Test
    void rejectsAnExplicitEmptyPort() {
        assertThrows(PluginConfig.ConfigException.class, () -> PluginConfig.parse(new StringReader("""
                url: http://example.com:/api/v1/lineage
                cluster: warehouse-a
                auth:
                  type: none
                """)));
    }

    @Test
    void rejectsAnUnusablePort() {
        assertThrows(PluginConfig.ConfigException.class, () -> PluginConfig.parse(new StringReader("""
                url: http://example.com:99999/api/v1/lineage
                cluster: warehouse-a
                auth:
                  type: none
                """)));
    }

    @Test
    void rejectsDuplicateRootKeys() {
        assertThrows(PluginConfig.ConfigException.class, () -> PluginConfig.parse(new StringReader("""
                url: https://marquez.example/api/v1/lineage
                timeoutMs: -1
                timeoutMs: 100
                cluster: warehouse-a
                auth:
                  type: none
                """)));
    }

    @Test
    void rejectsDuplicateNestedKeys() {
        assertThrows(PluginConfig.ConfigException.class, () -> PluginConfig.parse(new StringReader("""
                url: https://marquez.example/api/v1/lineage
                cluster: warehouse-a
                auth:
                  type: forbidden
                  type: none
                """)));
    }

    @Test
    void acceptsHttpSchemesRegardlessOfCase() throws Exception {
        PluginConfig upper = PluginConfig.parse(new StringReader("""
                url: HTTPS://example.com/api/v1/lineage
                cluster: warehouse-a
                auth:
                  type: none
                """));
        PluginConfig mixed = PluginConfig.parse(new StringReader("""
                url: Http://example.com/api/v1/lineage
                cluster: warehouse-a
                auth:
                  type: none
                """));

        assertEquals("https", upper.url().getScheme().toLowerCase(Locale.ROOT));
        assertEquals("/api/v1/lineage", upper.url().getPath());
        assertEquals("http", mixed.url().getScheme().toLowerCase(Locale.ROOT));
        assertEquals("/api/v1/lineage", mixed.url().getPath());
    }

    @Test
    void rejectsAnExplicitNullTokenWhenAuthIsNone() {
        assertThrows(PluginConfig.ConfigException.class, () -> PluginConfig.parse(new StringReader("""
                url: https://marquez.example/api/v1/lineage
                cluster: warehouse-a
                auth:
                  type: none
                  token: null
                """)));
        assertThrows(PluginConfig.ConfigException.class, () -> PluginConfig.parse(new StringReader("""
                url: https://marquez.example/api/v1/lineage
                cluster: warehouse-a
                auth:
                  type: none
                  token: ~
                """)));
    }

    @Test
    void rejectsABearerTokenWithHeaderControlCharacters() {
        String yaml = ""
                + "url: https://marquez.example/api/v1/lineage\n"
                + "cluster: warehouse-a\n"
                + "auth:\n"
                + "  type: bearer\n"
                + "  token: \"abc\\r\\nxyz\"\n";
        assertThrows(PluginConfig.ConfigException.class, () -> PluginConfig.parse(new StringReader(yaml)));
    }

    @Test
    void rejectsABearerTokenOutsideLatin1() {
        String yaml = ""
                + "url: https://marquez.example/api/v1/lineage\n"
                + "cluster: warehouse-a\n"
                + "auth:\n"
                + "  type: bearer\n"
                + "  token: \"\\u0100\"\n";
        assertThrows(PluginConfig.ConfigException.class, () -> PluginConfig.parse(new StringReader(yaml)));
    }

    @Test
    void rejectsARelativeUrl() {
        assertThrows(PluginConfig.ConfigException.class, () -> PluginConfig.parse(new StringReader("""
                url: /api/v1/lineage
                cluster: warehouse-a
                auth:
                  type: none
                """)));
    }
}
