package org.apache.doris.plugins.openlineage.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.CoercionAction;
import com.fasterxml.jackson.databind.cfg.CoercionInputShape;
import com.fasterxml.jackson.databind.type.LogicalType;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.yaml.snakeyaml.LoaderOptions;

import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.util.Set;

/**
 * Plugin-directory config. Only the four fields the spec allows are accepted.
 */
public final class PluginConfig {
    public static final int DEFAULT_TIMEOUT_MS = 5000;
    private static final Set<String> AUTH_TYPES = Set.of("none", "bearer");

    private final URI url;
    private final int timeoutMs;
    private final String cluster;
    private final Auth auth;

    private PluginConfig(URI url, int timeoutMs, String cluster, Auth auth) {
        this.url = url;
        this.timeoutMs = timeoutMs;
        this.cluster = cluster;
        this.auth = auth;
    }

    public static PluginConfig parse(Reader reader) throws ConfigException {
        LoaderOptions yaml = new LoaderOptions();
        yaml.setAllowDuplicateKeys(false);
        ObjectMapper mapper = new ObjectMapper(YAMLFactory.builder().loaderOptions(yaml).build())
                .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT);
        mapper.coercionConfigFor(LogicalType.Integer)
                .setAcceptBlankAsEmpty(true)
                .setCoercion(CoercionInputShape.EmptyString, CoercionAction.Fail)
                .setCoercion(CoercionInputShape.String, CoercionAction.Fail);
        Raw raw;
        try {
            raw = mapper.readValue(reader, Raw.class);
        } catch (JsonMappingException e) {
            throw new ConfigException("openlineage.yml has an unknown or illegal field: " + e.getOriginalMessage());
        } catch (IOException e) {
            throw new ConfigException("openlineage.yml could not be read: " + e.getMessage());
        }
        if (raw == null) {
            throw new ConfigException("openlineage.yml is empty");
        }
        return raw.toConfig();
    }

    public URI url() {
        return url;
    }

    public int timeoutMs() {
        return timeoutMs;
    }

    public String cluster() {
        return cluster;
    }

    public Auth auth() {
        return auth;
    }

    public String namespace() {
        return "doris://" + cluster;
    }

    public record Auth(String type, String token) {
        public boolean bearer() {
            return "bearer".equals(type);
        }
    }

    public static final class ConfigException extends Exception {
        public ConfigException(String message) {
            super(message);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = false)
    static final class Raw {
        public String url;
        public JsonNode timeoutMs;
        public String cluster;
        public RawAuth auth;

        PluginConfig toConfig() throws ConfigException {
            if (url == null || url.isBlank()) {
                throw new ConfigException("url is required");
            }
            URI uri;
            try {
                uri = URI.create(url.trim());
            } catch (IllegalArgumentException e) {
                throw new ConfigException("url is not an absolute http(s) POST endpoint");
            }
            if (!uri.isAbsolute() || uri.getScheme() == null
                    || !(uri.getScheme().equalsIgnoreCase("http") || uri.getScheme().equalsIgnoreCase("https"))
                    || uri.getHost() == null) {
                throw new ConfigException("url must be an absolute http(s) POST endpoint");
            }
            if (hasEmptyPort(uri) || (uri.getPort() != -1 && (uri.getPort() < 1 || uri.getPort() > 65535))) {
                throw new ConfigException("url port must be between 1 and 65535");
            }
            int timeout = readTimeout(timeoutMs);
            if (cluster == null || cluster.isBlank()) {
                throw new ConfigException("cluster is required");
            }
            if (auth == null) {
                throw new ConfigException("auth is required");
            }
            return new PluginConfig(uri, timeout, cluster.trim(), auth.toAuth());
        }

        private static boolean hasEmptyPort(URI uri) {
            String authority = uri.getRawAuthority();
            return authority != null && authority.endsWith(":");
        }

        private static int readTimeout(JsonNode timeoutMs) throws ConfigException {
            if (timeoutMs == null || timeoutMs.isMissingNode()) {
                return DEFAULT_TIMEOUT_MS;
            }
            if (!timeoutMs.isIntegralNumber() || !timeoutMs.canConvertToInt()) {
                throw new ConfigException("timeoutMs must be a positive integer");
            }
            int timeout = timeoutMs.intValue();
            if (timeout <= 0) {
                throw new ConfigException("timeoutMs must be a positive integer");
            }
            return timeout;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = false)
    static final class RawAuth {
        public String type;
        public JsonNode token;

        Auth toAuth() throws ConfigException {
            if (type == null || !AUTH_TYPES.contains(type)) {
                throw new ConfigException("auth.type must be none or bearer");
            }
            boolean tokenPresent = token != null && !token.isMissingNode();
            if ("none".equals(type) && tokenPresent) {
                throw new ConfigException("auth.token is not allowed when auth.type is none");
            }
            if (!"bearer".equals(type)) {
                return new Auth(type, null);
            }
            if (!tokenPresent || !token.isTextual() || token.asText().isBlank()) {
                throw new ConfigException("auth.token is required for bearer");
            }
            String value = token.asText();
            if (!isLegalHeaderValue("Bearer " + value)) {
                throw new ConfigException("auth.token contains a character that cannot appear in an HTTP header");
            }
            return new Auth(type, value);
        }

        /** Same range {@code HttpRequest} accepts for a header value: visible ASCII, space, and Latin-1. */
        private static boolean isLegalHeaderValue(String value) {
            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                if (c < 0x20 || c == 0x7F || c > 0xFF) {
                    return false;
                }
            }
            return true;
        }
    }
}
