package org.apache.doris.plugins.openlineage.doris;

import org.apache.doris.extension.spi.PluginContext;
import org.apache.doris.nereids.lineage.LineagePlugin;
import org.apache.doris.nereids.lineage.LineagePluginFactory;

public final class OpenLineagePluginFactory implements LineagePluginFactory {
    @Override
    public String name() {
        return OpenLineageLineagePlugin.NAME;
    }

    @Override
    public LineagePlugin create() {
        return new OpenLineageLineagePlugin();
    }

    @Override
    public LineagePlugin create(PluginContext context) {
        OpenLineageLineagePlugin plugin = new OpenLineageLineagePlugin();
        plugin.initialize(context);
        return plugin;
    }
}
