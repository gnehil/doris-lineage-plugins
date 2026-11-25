package org.apache.doris.plugins.openlineage.emit;

import org.apache.doris.plugins.openlineage.convert.EventEncoder.Event;
import org.apache.doris.plugins.openlineage.emit.EventSink.Receipt;

import java.util.List;
import java.util.function.LongSupplier;

public final class EventDelivery {
    private EventDelivery() {
    }

    public static Drop send(List<Event> events, EventSink sink, LongSupplier clock) {
        SendBudget budget = new SendBudget(clock);
        for (Event event : events) {
            Receipt receipt = budget.remaining().isZero()
                    ? Receipt.failure(0, null, "timeout")
                    : sink.send(event, budget);
            if (!receipt.acknowledged()) {
                return new Drop(event.type(), event.runId(), receipt);
            }
        }
        return null;
    }

    public record Drop(String eventType, String runId, Receipt receipt) {
    }
}
