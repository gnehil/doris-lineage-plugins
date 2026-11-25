package org.apache.doris.plugins.openlineage.kafka;

import org.apache.doris.plugins.openlineage.config.ConfigYaml.ConfigException;
import org.apache.doris.plugins.openlineage.config.EventFormat;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

import java.io.StringReader;

import static org.junit.jupiter.api.Assertions.*;

class KafkaConfigTest {
    private static final String CONFIG = """
            cluster: warehouse-a
            bootstrapServers: [kafka-1:9092, kafka-2:9092]
            topic: lineage
            """;

    @Test
    void defaultsToDorisJsonAndRequiresAllBrokerAcknowledgements() throws ConfigException {
        var config = KafkaConfig.parse(new StringReader(CONFIG));
        assertEquals(EventFormat.DORIS_JSON, config.format());
        assertEquals("kafka-1:9092,kafka-2:9092", config.properties().get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("all", config.properties().get(ProducerConfig.ACKS_CONFIG));
        assertEquals(5_000L, config.properties().get(ProducerConfig.MAX_BLOCK_MS_CONFIG));
    }

    @Test
    void acceptsOpenLineageAndKafkaSecurityProperties() throws ConfigException {
        var config = KafkaConfig.parse(new StringReader(CONFIG + """
                format: openlineage
                timeoutMs: 20000
                producerProperties:
                  security.protocol: SSL
                  ssl.truststore.password: secret
                """));
        assertEquals(EventFormat.OPENLINEAGE, config.format());
        assertEquals("SSL", config.properties().get("security.protocol"));
        assertEquals(15_000L, config.properties().get(ProducerConfig.MAX_BLOCK_MS_CONFIG));
    }

    @Test
    void forbidsOverridingManagedSettingsAndRedactsValues() {
        var error = assertThrows(ConfigException.class, () -> KafkaConfig.parse(new StringReader(CONFIG + """
                producerProperties:
                  acks: secret-value
                """)));
        assertTrue(error.getMessage().contains("producerProperties.acks"));
        assertFalse(error.getMessage().contains("secret-value"));
    }
}
