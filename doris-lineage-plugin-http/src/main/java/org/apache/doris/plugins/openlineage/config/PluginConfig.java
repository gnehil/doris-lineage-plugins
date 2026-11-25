package org.apache.doris.plugins.openlineage.config;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.doris.plugins.openlineage.config.ConfigYaml.ConfigException;

import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.util.Set;

public final class PluginConfig {
    private static final Set<String> AUTH_TYPES = Set.of("none", "bearer");

    private final URI url;
    private final int timeoutMs;
    private final String cluster;
    private final Auth auth;
    private final EventFormat format;

    private PluginConfig(URI url, int timeoutMs, String cluster, Auth auth, EventFormat format) {
        this.url = url;
        this.timeoutMs = timeoutMs;
        this.cluster = cluster;
        this.auth = auth;
        this.format = format;
    }

    public static PluginConfig parse(Reader reader) throws ConfigException {
        return ConfigYaml.read(reader, Raw.class).toConfig();
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

    public EventFormat format() {
        return format;
    }

    public record Auth(String type, String token) {
        public boolean bearer() {
            return "bearer".equals(type);
        }
    }

    static final class Raw {
        public String url;
        public JsonNode timeoutMs;
        public JsonNode format;
        @JsonDeserialize(using = ClusterNameDeserializer.class)
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
            int timeout = ConfigYaml.timeoutMs(timeoutMs);
            String clusterName = ConfigYaml.required(cluster, "cluster");
            if (auth == null) {
                throw new ConfigException("auth is required");
            }
            return new PluginConfig(uri, timeout, clusterName, auth.toAuth(),
                    EventFormat.parse(format, EventFormat.OPENLINEAGE));
        }

        private static boolean hasEmptyPort(URI uri) {
            String authority = uri.getRawAuthority();
            return authority != null && authority.endsWith(":");
        }

    }

    static final class ClusterNameDeserializer extends JsonDeserializer<String> {
        @Override
        public String deserialize(JsonParser parser, DeserializationContext context) throws IOException {
            // Keep the scalar spelling accepted by the original HTTP configuration.
            if (parser.currentToken().isScalarValue()) {
                return parser.getValueAsString();
            }
            return context.reportInputMismatch(String.class, "cluster must be a scalar");
        }
    }

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
