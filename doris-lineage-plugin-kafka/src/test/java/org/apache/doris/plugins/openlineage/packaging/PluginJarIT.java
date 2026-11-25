package org.apache.doris.plugins.openlineage.packaging;

import org.apache.doris.extension.spi.PluginContext;
import org.apache.doris.nereids.lineage.LineagePluginFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.jar.JarFile;

import static org.junit.jupiter.api.Assertions.*;

class PluginJarIT {
    @Test
    void containsOnlyItsOwnSinkAndDeclaresTheDorisFactory() throws IOException {
        String factory = "org.apache.doris.plugins.openlineage.kafka.KafkaLineagePluginFactory";
        String spi = "META-INF/services/org.apache.doris.nereids.lineage.LineagePluginFactory";
        try (JarFile jar = new JarFile(System.getProperty("plugin.jar"))) {
            assertEquals("1.0", jar.getManifest().getMainAttributes().getValue("Doris-Lineage-Plugin-Api-Version"));
            assertNotNull(jar.getJarEntry(factory.replace('.', '/') + ".class"));
            assertNotNull(jar.getJarEntry("org/apache/doris/plugins/openlineage/doris/DorisLineageAdapter.class"));
            var descriptor = jar.getJarEntry(spi);
            assertNotNull(descriptor, spi);
            try (var stream = jar.getInputStream(descriptor)) {
                assertEquals(factory, new String(stream.readAllBytes(), StandardCharsets.UTF_8).strip());
            }
            var classes = jar.stream().map(entry -> entry.getName()).filter(name -> name.endsWith(".class")).toList();
            assertTrue(classes.stream().anyMatch(name -> name.startsWith("org/apache/kafka/clients/")));
            assertFalse(classes.stream().anyMatch(name -> name.startsWith("org/apache/hc/")));
            assertFalse(classes.stream().anyMatch(name -> name.startsWith("org/apache/doris/")
                    && !name.startsWith("org/apache/doris/plugins/openlineage/")));
        }
    }

    @Test
    void initializesScramFromTheJarAndRestoresTheFeContextLoader(@TempDir Path directory) throws IOException {
        String loginModule = "org.apache.kafka.common.security.scram.ScramLoginModule";
        String yaml = """
                cluster: warehouse-a
                bootstrapServers: 127.0.0.1:9092
                topic: lineage
                producerProperties:
                  security.protocol: SASL_PLAINTEXT
                  sasl.mechanism: SCRAM-SHA-512
                  sasl.jaas.config: '%s required username="lineage" password="test-password";'
                """;
        Path config = directory.resolve("kafka.yml");
        var context = new PluginContext(Map.of("plugin.path", directory.toString()));
        var feLoader = new ClassLoader(ClassLoader.getPlatformClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (name.startsWith("org.apache.doris.extension.spi.") || name.startsWith("org.apache.doris.nereids.lineage.")) {
                    return LineagePluginFactory.class.getClassLoader().loadClass(name);
                }
                return super.loadClass(name, resolve);
            }
        };
        assertThrows(ClassNotFoundException.class, () -> feLoader.loadClass(loginModule));
        URL jar = Path.of(System.getProperty("plugin.jar")).toUri().toURL();
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        try (var pluginLoader = new URLClassLoader(new URL[] {jar}, feLoader)) {
            thread.setContextClassLoader(feLoader);
            var factory = ServiceLoader.load(LineagePluginFactory.class, pluginLoader).findFirst().orElseThrow();
            Files.writeString(config, yaml.formatted(loginModule));
            try (var plugin = factory.create()) {
                plugin.initialize(context);
                assertTrue(plugin.eventFilter());
                assertSame(feLoader, thread.getContextClassLoader());
            }
            Files.writeString(config, yaml.formatted("missing.LoginModule"));
            try (var plugin = factory.create()) {
                var error = assertThrows(IllegalStateException.class, () -> plugin.initialize(context));
                assertFalse(error.getMessage().contains("test-password"));
                assertSame(feLoader, thread.getContextClassLoader());
            }
        } finally {
            thread.setContextClassLoader(previous);
        }
    }
}
