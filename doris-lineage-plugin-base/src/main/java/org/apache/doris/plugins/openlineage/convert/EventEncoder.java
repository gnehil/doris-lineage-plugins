package org.apache.doris.plugins.openlineage.convert;

import java.util.List;

public interface EventEncoder {
    boolean requiresSchemas();

    ConversionResult convert(LineageInput input);

    record Event(String type, String runId, byte[] body) {
    }

    record Diagnostic(String queryId, String targetColumn, String reason) {
    }

    record ConversionResult(List<Event> events, List<Diagnostic> diagnostics,
                            boolean lineageComplete, String startTimeSource) {
    }
}
