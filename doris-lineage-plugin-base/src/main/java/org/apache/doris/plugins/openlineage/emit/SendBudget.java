package org.apache.doris.plugins.openlineage.emit;

import java.time.Duration;
import java.util.function.LongSupplier;

public final class SendBudget {
    public static final Duration TOTAL = Duration.ofSeconds(15);

    private final LongSupplier clock;
    private final long deadline;

    SendBudget(LongSupplier clock) {
        this.clock = clock;
        this.deadline = clock.getAsLong() + TOTAL.toNanos();
    }

    public Duration remaining() {
        return Duration.ofNanos(Math.max(0, deadline - clock.getAsLong()));
    }
}
