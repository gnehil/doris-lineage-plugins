package org.apache.doris.plugins.openlineage.kafka;

import org.apache.doris.nereids.lineage.LineagePlugin;
import org.apache.doris.nereids.lineage.LineagePluginFactory;

public final class KafkaLineagePluginFactory implements LineagePluginFactory {
    @Override
    public String name() {
        return KafkaLineagePlugin.NAME;
    }

    @Override
    public LineagePlugin create() {
        return new KafkaLineagePlugin();
    }
}
