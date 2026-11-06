package org.apache.doris.plugins.openlineage.packaging;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarFile;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PluginJarIT {
    @Test
    void currentPackageContainsNoDorisFeClasses() throws IOException {
        Path jar = Path.of("target/openlineage-plugin-0.1.0-SNAPSHOT.jar");
        assertTrue(Files.isRegularFile(jar), "mvn verify must package the current plugin jar before this check");
        try (JarFile file = new JarFile(jar.toFile())) {
            boolean dorisClass = file.stream().anyMatch(entry -> PluginPackageTest.isDorisClass(entry.getName()));
            assertFalse(dorisClass, "plugin jar contains Doris FE classes");
        }
    }
}
