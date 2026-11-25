package org.apache.doris.plugins.openlineage.convert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

final class ColumnLineageSchema {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final JsonSchema SCHEMA = load();

    private ColumnLineageSchema() {
    }

    static void assertValid(JsonNode facet) {
        Set<ValidationMessage> errors = SCHEMA.validate(JSON.createObjectNode().set("columnLineage", facet));
        if (!errors.isEmpty()) {
            throw new AssertionError(errors.toString());
        }
    }

    private static JsonSchema load() {
        String openLineage = read("/openlineage/OpenLineage-2-0-2.json");
        String facetSchema = read("/openlineage/ColumnLineageDatasetFacet-1-2-0.json");
        return JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012, builder -> builder.schemaLoaders(
                loaders -> loaders.schemas(Map.of(
                        "https://openlineage.io/spec/2-0-2/OpenLineage.json", openLineage,
                        "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json", facetSchema))))
                .getSchema(facetSchema);
    }

    private static String read(String resource) {
        try (InputStream in = ColumnLineageSchema.class.getResourceAsStream(resource)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException(resource, e);
        }
    }
}
