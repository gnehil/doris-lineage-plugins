package org.apache.doris.plugins.openlineage.doris;

import org.apache.doris.extension.spi.PluginContext;
import org.apache.doris.nereids.lineage.LineageInfo;
import org.apache.doris.nereids.lineage.LineagePlugin;
import org.apache.doris.plugins.openlineage.config.PluginConfig;
import org.apache.doris.plugins.openlineage.convert.EventConverter;
import org.apache.doris.plugins.openlineage.emit.EventSender;
import org.apache.doris.plugins.openlineage.emit.LineageHttpPoster;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class OpenLineageLineagePlugin implements LineagePlugin {
    static final String NAME = "openlineage";
    private static final Logger LOG = Logger.getLogger(OpenLineageLineagePlugin.class.getName());

    private volatile boolean ready;
    private PluginConfig config;
    private EventConverter converter;
    private EventSender sender;
    private LineageHttpPoster poster;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void initialize(PluginContext context) {
        String pluginPath = context == null ? null : context.getProperties().get("plugin.path");
        if (pluginPath == null || pluginPath.isBlank()) {
            throw new IllegalStateException("plugin.path is required");
        }
        Path file = Path.of(pluginPath, "openlineage.yml");
        try (Reader reader = Files.newBufferedReader(file)) {
            config = PluginConfig.parse(reader);
        } catch (Exception e) {
            throw new IllegalStateException("openlineage.yml is invalid: " + file, e);
        }
        converter = new EventConverter(UUID::randomUUID);
        poster = new LineageHttpPoster(Duration.ofMillis(config.timeoutMs()));
        sender = new EventSender(poster, System::nanoTime, duration -> {
            try {
                Thread.sleep(duration.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        ready = true;
    }

    @Override
    public boolean eventFilter() {
        return ready;
    }

    @Override
    public boolean exec(LineageInfo lineageInfo) {
        if (!ready || lineageInfo == null) {
            return false;
        }
        String queryId = lineageInfo.getContext() == null ? null : lineageInfo.getContext().getQueryId();
        try {
            var input = DorisLineageAdapter.toInput(lineageInfo, config.cluster(), System.currentTimeMillis());
            var converted = converter.convert(input);
            for (var diagnostic : converted.diagnostics()) {
                LOG.log(Level.WARNING, "query_id={0} target_column={1} reason={2}",
                        new Object[] {diagnostic.queryId(), diagnostic.targetColumn(), diagnostic.reason()});
            }
            if (!converted.lineageComplete()) {
                LOG.warning("query_id=" + queryId + " conversion incomplete");
            }
            if (converted.events().size() != 2) {
                return false;
            }
            String target = input.output() == null ? null
                    : input.output().catalog() + "." + input.output().database() + "." + input.output().table();
            var sent = sender.send(config, queryId, target, converted.events().get(0), converted.events().get(1));
            if (!sent.sent() && sent.drop() != null) {
                LOG.log(Level.WARNING,
                        "query_id={0} target_table={1} event_type={2} run_id={3} attempts={4} http_status={5} error_type={6}",
                        new Object[] {sent.drop().queryId(), sent.drop().targetTable(), sent.drop().eventType(),
                                sent.drop().runId(), sent.drop().attempts(), sent.drop().httpStatus(),
                                sent.drop().errorType()});
            }
            return sent.sent();
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "query_id=" + queryId + " lineage emit failed", e);
            return false;
        }
    }

    @Override
    public void close() {
        ready = false;
        if (poster != null) {
            try {
                poster.close();
            } catch (IOException e) {
                LOG.log(Level.WARNING, "failed to close lineage HTTP client", e);
            }
        }
    }
}
