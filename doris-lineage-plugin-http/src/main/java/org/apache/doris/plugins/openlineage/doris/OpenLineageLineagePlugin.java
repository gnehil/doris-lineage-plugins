package org.apache.doris.plugins.openlineage.doris;

import org.apache.doris.plugins.openlineage.config.ConfigYaml.ConfigException;
import org.apache.doris.plugins.openlineage.config.PluginConfig;
import org.apache.doris.plugins.openlineage.emit.EventSender;
import org.apache.doris.plugins.openlineage.emit.LineageHttpPoster;

import java.io.Reader;
import java.time.Duration;

public final class OpenLineageLineagePlugin extends DorisLineagePlugin {
    static final String NAME = "openlineage";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    protected Session open(Reader reader) throws ConfigException {
        PluginConfig config = PluginConfig.parse(reader);
        var encoder = config.format().encoder();
        var poster = new LineageHttpPoster(Duration.ofMillis(config.timeoutMs()));
        return new Session(config.cluster(), encoder, new EventSender(config, poster, duration -> {
            try {
                Thread.sleep(duration.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
    }
}
