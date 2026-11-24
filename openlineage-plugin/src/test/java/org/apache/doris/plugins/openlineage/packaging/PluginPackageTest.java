package org.apache.doris.plugins.openlineage.packaging;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

class PluginPackageTest {
    @Test
    void coreSourcesDoNotImportDoris() throws IOException {
        Path root = Path.of("src/main/java");
        try (Stream<Path> files = Files.walk(root)) {
            files.filter(path -> path.toString().endsWith(".java")).forEach(path -> {
                try {
                    String source = Files.readString(path);
                    assertTrue(!source.lines().anyMatch(line -> line.startsWith("import org.apache.doris.")
                                    && !line.startsWith("import org.apache.doris.plugins.openlineage.")),
                            path + " imports Doris types");
                } catch (IOException e) {
                    throw new IllegalStateException(path.toString(), e);
                }
            });
        }
    }

    @Test
    void serviceLoaderNamesTheFactory() throws IOException {
        String services = Files.readString(Path.of(
                "src/doris/resources/META-INF/services/org.apache.doris.nereids.lineage.LineagePluginFactory"));
        assertTrue(services.contains("org.apache.doris.plugins.openlineage.doris.OpenLineagePluginFactory"));
    }

    static boolean isDorisClass(String name) {
        return name.endsWith(".class")
                && name.startsWith("org/apache/doris/")
                && !name.startsWith("org/apache/doris/plugins/openlineage/");
    }
}
