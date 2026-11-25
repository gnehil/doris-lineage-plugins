package org.apache.doris.plugins.openlineage.doris;

import org.apache.doris.extension.spi.PluginContext;
import org.apache.doris.nereids.lineage.LineageInfo;
import org.apache.doris.nereids.lineage.LineagePlugin;
import org.apache.doris.plugins.openlineage.config.ConfigYaml.ConfigException;
import org.apache.doris.plugins.openlineage.convert.EventEncoder;
import org.apache.doris.plugins.openlineage.emit.EventDelivery;
import org.apache.doris.plugins.openlineage.emit.EventSink;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class DorisLineagePlugin implements LineagePlugin {
    private final Logger log = Logger.getLogger(getClass().getName());
    private volatile Session session;

    protected abstract Session open(Reader reader) throws ConfigException;

    @Override
    public final synchronized void initialize(PluginContext context) {
        if (session != null) {
            throw new IllegalStateException("lineage plugin already initialized");
        }
        String pluginPath = context == null ? null : context.getProperties().get("plugin.path");
        if (pluginPath == null || pluginPath.isBlank()) {
            throw new IllegalStateException("plugin.path is required for " + name());
        }
        Path file = Path.of(pluginPath, name() + ".yml");
        try (Reader reader = Files.newBufferedReader(file)) {
            session = open(reader);
        } catch (ConfigException e) {
            throw new IllegalStateException(file + " " + e.getMessage());
        } catch (IOException e) {
            close();
            throw new IllegalStateException(file + " could not be read");
        }
    }

    @Override
    public final boolean eventFilter() {
        return session != null;
    }

    @Override
    public final boolean exec(LineageInfo info) {
        Session current = session;
        if (current == null || info == null) {
            return false;
        }
        String queryId = info.getContext() == null ? null : info.getContext().getQueryId();
        try {
            EventEncoder encoder = current.encoder();
            var input = DorisLineageAdapter.toInput(info, current.cluster(), System.currentTimeMillis(),
                    encoder.requiresSchemas());
            var converted = encoder.convert(input);
            for (var diagnostic : converted.diagnostics()) {
                log.log(Level.WARNING, "query_id={0} target_column={1} reason={2}",
                        new Object[] {diagnostic.queryId(), diagnostic.targetColumn(), diagnostic.reason()});
            }
            if (!converted.lineageComplete()) {
                log.log(Level.WARNING, "query_id={0} conversion incomplete", queryId);
            }
            if (converted.events().isEmpty()) {
                return false;
            }
            var drop = EventDelivery.send(converted.events(), current.sink(), System::nanoTime);
            if (drop != null) {
                log.log(Level.WARNING,
                        "query_id={0} target_table={1} event_type={2} run_id={3} attempts={4} http_status={5} error_type={6}",
                        new Object[] {queryId, input.output().qualifiedName(), drop.eventType(), drop.runId(),
                                drop.receipt().attempts(), drop.receipt().status(), drop.receipt().errorType()});
            }
            return drop == null;
        } catch (RuntimeException e) {
            log.log(Level.WARNING, "query_id={0} lineage emit failed ({1})",
                    new Object[] {queryId, e.getClass().getSimpleName()});
            return false;
        }
    }

    @Override
    public final synchronized void close() {
        Session current = session;
        session = null;
        if (current != null) {
            try {
                current.sink().close();
            } catch (IOException | RuntimeException e) {
                log.warning("failed to close lineage sink");
            }
        }
    }

    protected record Session(String cluster, EventEncoder encoder, EventSink sink) {
        public Session {
        }
    }
}
