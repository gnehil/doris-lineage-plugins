package org.apache.doris.plugins.openlineage.packaging;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.jar.JarFile;

import static org.junit.jupiter.api.Assertions.*;

class PluginJarIT {
    @Test
    void containsOnlyItsOwnSinkAndDeclaresTheDorisFactory() throws IOException {
        String factory = "org.apache.doris.plugins.openlineage.doris.OpenLineagePluginFactory";
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
            assertTrue(classes.stream().anyMatch(name -> name.startsWith("org/apache/hc/client5/")));
            assertFalse(classes.stream().anyMatch(name -> name.startsWith("org/apache/kafka/")));
            assertFalse(classes.stream().anyMatch(name -> name.startsWith("org/apache/doris/")
                    && !name.startsWith("org/apache/doris/plugins/openlineage/")));
        }
    }
}
