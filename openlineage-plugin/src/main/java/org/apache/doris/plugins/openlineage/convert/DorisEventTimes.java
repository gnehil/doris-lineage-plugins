package org.apache.doris.plugins.openlineage.convert;

/**
 * Doris 4.0.6 stores {@code timestampMs} as the lineage event time, which is when the query
 * finished, and {@code durationMs} as elapsed query time. It does not store a separate start time.
 */
public final class DorisEventTimes {
    public static final long UNKNOWN = -1L;

    private DorisEventTimes() {
    }

    public record Window(Long startMs, Long durationMs) {
    }

    public static Window from(long timestampMs, long durationMs) {
        if (timestampMs == UNKNOWN) {
            return new Window(null, null);
        }
        if (durationMs == UNKNOWN || durationMs < 0) {
            return new Window(timestampMs, 0L);
        }
        try {
            return new Window(Math.subtractExact(timestampMs, durationMs), durationMs);
        } catch (ArithmeticException e) {
            return new Window(timestampMs, 0L);
        }
    }
}
