package org.apache.doris.plugins.openlineage.config;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.CoercionAction;
import com.fasterxml.jackson.databind.cfg.CoercionInputShape;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.databind.type.LogicalType;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.yaml.snakeyaml.LoaderOptions;

import java.io.IOException;
import java.io.Reader;

public final class ConfigYaml {
    private static final int DEFAULT_TIMEOUT_MS = 5_000;
    private static final ObjectMapper MAPPER = mapper();

    private ConfigYaml() {
    }

    private static ObjectMapper mapper() {
        LoaderOptions yaml = new LoaderOptions();
        yaml.setAllowDuplicateKeys(false);
        ObjectMapper mapper = new ObjectMapper(YAMLFactory.builder().loaderOptions(yaml).build())
                .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        mapper.coercionConfigFor(LogicalType.Textual)
                .setCoercion(CoercionInputShape.Integer, CoercionAction.Fail)
                .setCoercion(CoercionInputShape.Float, CoercionAction.Fail)
                .setCoercion(CoercionInputShape.Boolean, CoercionAction.Fail);
        return mapper;
    }

    public static <T> T read(Reader reader, Class<T> type) throws ConfigException {
        try {
            T value = MAPPER.readValue(reader, type);
            if (value == null) {
                throw new ConfigException("configuration is empty");
            }
            return value;
        } catch (UnrecognizedPropertyException e) {
            throw new ConfigException("unknown field " + e.getPropertyName());
        } catch (JsonMappingException e) {
            String field = e.getPath().stream().map(JsonMappingException.Reference::getFieldName)
                    .filter(name -> name != null).reduce((parent, child) -> parent + "." + child).orElse("configuration");
            throw new ConfigException(field + " has an invalid value");
        } catch (IOException e) {
            throw new ConfigException("invalid YAML");
        }
    }

    public static String required(String value, String field) throws ConfigException {
        if (value == null || value.isBlank()) {
            throw new ConfigException(field + " is required");
        }
        return value.trim();
    }

    public static int timeoutMs(JsonNode timeout) throws ConfigException {
        if (timeout == null) {
            return DEFAULT_TIMEOUT_MS;
        }
        if (!timeout.isIntegralNumber() || !timeout.canConvertToInt() || timeout.intValue() <= 0) {
            throw new ConfigException("timeoutMs must be a positive integer");
        }
        return timeout.intValue();
    }

    public static final class ConfigException extends Exception {
        public ConfigException(String message) {
            super(message);
        }
    }
}
