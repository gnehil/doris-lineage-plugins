package org.apache.doris.plugins.openlineage.config;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.doris.plugins.openlineage.config.ConfigYaml.ConfigException;
import org.apache.doris.plugins.openlineage.convert.DorisJsonEncoder;
import org.apache.doris.plugins.openlineage.convert.EventConverter;
import org.apache.doris.plugins.openlineage.convert.EventEncoder;

import java.util.UUID;

public enum EventFormat {
    DORIS_JSON,
    OPENLINEAGE;

    public static EventFormat parse(JsonNode value, EventFormat defaultFormat) throws ConfigException {
        if (value == null) {
            return defaultFormat;
        }
        return switch (value.asText()) {
            case "doris-json" -> DORIS_JSON;
            case "openlineage" -> OPENLINEAGE;
            default -> throw new ConfigException("format must be doris-json or openlineage");
        };
    }

    public EventEncoder encoder() {
        return switch (this) {
            case DORIS_JSON -> new DorisJsonEncoder(UUID::randomUUID);
            case OPENLINEAGE -> new EventConverter(UUID::randomUUID);
        };
    }
}
