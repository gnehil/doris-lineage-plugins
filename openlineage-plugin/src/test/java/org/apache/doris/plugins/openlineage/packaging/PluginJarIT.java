package org.apache.doris.plugins.openlineage.packaging;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PluginJarIT {
    private static final Path PLUGIN_JAR = Path.of("target/openlineage-plugin-0.1.0-SNAPSHOT.jar");

    @Test
    void currentPackageContainsNoDorisFeClasses() throws IOException {
        try (JarFile file = openPluginJar()) {
            boolean dorisClass = file.stream().anyMatch(entry -> PluginPackageTest.isDorisClass(entry.getName()));
            assertFalse(dorisClass, "plugin jar contains Doris FE classes");
        }
    }

    @Test
    void shadedJarDeclaresLineagePluginApiVersion() throws IOException {
        try (JarFile file = openPluginJar()) {
            Manifest manifest = file.getManifest();
            assertNotNull(manifest, "shaded jar has no MANIFEST.MF");
            Attributes attributes = manifest.getMainAttributes();
            assertEquals("1.0", attributes.getValue("Doris-Lineage-Plugin-Api-Version"));
        }
    }

    @Test
    void shadedJarContainsAndRegistersLineagePluginFactory() throws IOException {
        String factory = "org.apache.doris.plugins.openlineage.doris.OpenLineagePluginFactory";
        String service = "META-INF/services/org.apache.doris.nereids.lineage.LineagePluginFactory";
        try (JarFile file = openPluginJar()) {
            assertNotNull(file.getJarEntry(factory.replace('.', '/') + ".class"),
                    "plugin jar is missing the factory class; run mvn clean verify with -Ddoris.fe.lib=/path/to/fe/lib");
            JarEntry descriptor = file.getJarEntry(service);
            assertNotNull(descriptor, "plugin jar is missing " + service);
            try (InputStream input = file.getInputStream(descriptor)) {
                assertEquals(factory, new String(input.readAllBytes(), StandardCharsets.UTF_8).strip());
            }
        }
    }

    private static JarFile openPluginJar() throws IOException {
        assertTrue(Files.isRegularFile(PLUGIN_JAR), "mvn verify must package the current plugin jar before this check");
        return new JarFile(PLUGIN_JAR.toFile());
    }
}
