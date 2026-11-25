package org.apache.doris.plugins.openlineage.emit;

import org.apache.doris.plugins.openlineage.convert.EventEncoder.Event;

import java.io.Closeable;

public interface EventSink extends Closeable {
    Receipt send(Event event, SendBudget budget);

    record Receipt(boolean acknowledged, int attempts, Integer status, String errorType) {
        public static Receipt success(int attempts) {
            return new Receipt(true, attempts, null, null);
        }

        public static Receipt failure(int attempts, Integer status, String errorType) {
            return new Receipt(false, attempts, status, errorType);
        }
    }
}
