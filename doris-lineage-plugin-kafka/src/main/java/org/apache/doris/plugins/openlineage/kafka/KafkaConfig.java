package org.apache.doris.plugins.openlineage.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.doris.plugins.openlineage.config.ConfigYaml;
import org.apache.doris.plugins.openlineage.config.ConfigYaml.ConfigException;
import org.apache.doris.plugins.openlineage.config.EventFormat;
import org.apache.doris.plugins.openlineage.emit.SendBudget;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

record KafkaConfig(String cluster, EventFormat format, int timeoutMs, String topic, Map<String, Object> properties) {
    static KafkaConfig parse(Reader reader) throws ConfigException {
        Raw raw = ConfigYaml.read(reader, Raw.class);
        String cluster = ConfigYaml.required(raw.cluster, "cluster");
        EventFormat format = EventFormat.parse(raw.format, EventFormat.DORIS_JSON);
        int timeout = ConfigYaml.timeoutMs(raw.timeoutMs);
        String topic = ConfigYaml.required(raw.topic, "topic");
        if (topic.equals(".") || topic.equals("..") || !topic.matches("[a-zA-Z0-9._-]{1,249}")) {
            throw new ConfigException("topic is not a valid Kafka topic");
        }

        Map<String, Object> properties = new HashMap<>();
        if (raw.producerProperties != null) {
            if (!raw.producerProperties.isObject()) {
                throw new ConfigException("producerProperties must be a mapping of Kafka TLS/SASL settings");
            }
            var fields = raw.producerProperties.fields();
            while (fields.hasNext()) {
                var field = fields.next();
                String key = field.getKey();
                if (!(key.equals("security.protocol") || key.startsWith("ssl.") || key.startsWith("sasl."))
                        || !ProducerConfig.configNames().contains(key)) {
                    throw new ConfigException("producerProperties." + key + " is not a supported TLS/SASL setting");
                }
                JsonNode value = field.getValue();
                if (!(value.isTextual() || value.isNumber() || value.isBoolean())) {
                    throw new ConfigException("producerProperties." + key + " must be a scalar");
                }
                properties.put(key, value.asText());
            }
        }
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers(raw.bootstrapServers));
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        int maxWait = (int) Math.min(timeout, SendBudget.TOTAL.toMillis());
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, (long) maxWait);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, maxWait);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, maxWait);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 0L);
        return new KafkaConfig(cluster, format, timeout, topic, Map.copyOf(properties));
    }

    private static String servers(JsonNode value) throws ConfigException {
        if (value != null && value.isArray() && !value.isEmpty()) {
            var entries = new ArrayList<String>();
            for (JsonNode item : value) {
                if (!item.isTextual() || item.asText().isBlank() || item.asText().contains(",")) {
                    throw new ConfigException("bootstrapServers must contain Kafka host:port entries");
                }
                entries.add(item.asText().trim());
            }
            return String.join(",", entries);
        }
        if (value != null && value.isTextual() && !value.asText().isBlank()) {
            for (String entry : value.asText().split(",", -1)) {
                if (entry.isBlank()) {
                    throw new ConfigException("bootstrapServers must contain Kafka host:port entries");
                }
            }
            return value.asText().trim();
        }
        throw new ConfigException("bootstrapServers requires Kafka host:port entries");
    }

    static final class Raw {
        public String cluster;
        public JsonNode format;
        public JsonNode timeoutMs;
        public JsonNode bootstrapServers;
        public String topic;
        public JsonNode producerProperties;
    }
}
