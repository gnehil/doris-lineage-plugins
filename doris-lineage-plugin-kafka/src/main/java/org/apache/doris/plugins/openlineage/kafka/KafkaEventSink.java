package org.apache.doris.plugins.openlineage.kafka;

import org.apache.doris.plugins.openlineage.convert.EventEncoder.Event;
import org.apache.doris.plugins.openlineage.emit.EventSink;
import org.apache.doris.plugins.openlineage.emit.SendBudget;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

final class KafkaEventSink implements EventSink {
    private final Producer<String, byte[]> producer;
    private final String topic;
    private final Duration timeout;
    private final ThreadPoolExecutor worker;
    private final AtomicBoolean inFlight = new AtomicBoolean();

    KafkaEventSink(Producer<String, byte[]> producer, String topic, Duration timeout) {
        this.producer = producer;
        this.topic = topic;
        this.timeout = timeout;
        this.worker = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1), task -> {
            Thread thread = new Thread(task, "doris-lineage-kafka");
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public Receipt send(Event event, SendBudget budget) {
        long deadline = System.nanoTime() + Math.min(timeout.toNanos(), budget.remaining().toNanos());
        if (remaining(deadline, budget) <= 0 || Thread.currentThread().isInterrupted()) {
            return Receipt.failure(0, null, "timeout");
        }
        if (!inFlight.compareAndSet(false, true)) {
            return Receipt.failure(0, null, "producer_busy");
        }

        // Entering producer.send and cancelling a pending send compete for this claim.
        var sendClaimed = new AtomicBoolean();
        var delivery = new Delivery(() -> deliver(event, deadline, budget, sendClaimed));
        try {
            worker.execute(delivery);
        } catch (RejectedExecutionException e) {
            inFlight.set(false);
            return Receipt.failure(0, null, worker.isShutdown() ? "closed" : "producer_busy");
        }
        try {
            return delivery.await(remaining(deadline, budget));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Receipt.failure(sendClaimed.getAndSet(true) ? 1 : 0, null, "interrupted");
        } catch (TimeoutException e) {
            return Receipt.failure(sendClaimed.getAndSet(true) ? 1 : 0, null, "timeout");
        } catch (CancellationException e) {
            return Receipt.failure(0, null, "closed");
        } catch (ExecutionException e) {
            return kafkaFailure(e.getCause());
        } finally {
            delivery.cancel(true);
        }
    }

    private Receipt deliver(Event event, long deadline, SendBudget budget, AtomicBoolean sendClaimed)
            throws InterruptedException, TimeoutException {
        if (remaining(deadline, budget) <= 0 || Thread.currentThread().isInterrupted()
                || !sendClaimed.compareAndSet(false, true)) {
            return Receipt.failure(0, null, "timeout");
        }
        // send() may block on metadata or buffer allocation before returning an acknowledgement future.
        var acknowledgement = producer.send(new ProducerRecord<>(topic, event.runId(), event.body()));
        try {
            acknowledgement.get(remaining(deadline, budget), TimeUnit.NANOSECONDS);
            return remaining(deadline, budget) > 0 ? Receipt.success(1) : Receipt.failure(1, null, "timeout");
        } catch (ExecutionException e) {
            return kafkaFailure(e.getCause());
        }
    }

    private static Receipt kafkaFailure(Throwable cause) {
        if (cause instanceof TimeoutException || cause instanceof org.apache.kafka.common.errors.TimeoutException) {
            return Receipt.failure(1, null, "timeout");
        }
        if (cause instanceof InterruptedException || cause instanceof org.apache.kafka.common.errors.InterruptException) {
            return Receipt.failure(1, null, "interrupted");
        }
        return Receipt.failure(1, null, "kafka");
    }

    private static long remaining(long deadline, SendBudget budget) {
        return Math.max(0, Math.min(deadline - System.nanoTime(), budget.remaining().toNanos()));
    }

    @Override
    public void close() {
        worker.shutdownNow().forEach(task -> ((Delivery) task).discard());
        producer.close(Duration.ZERO);
    }

    private final class Delivery extends FutureTask<Receipt> {
        private final CountDownLatch finished = new CountDownLatch(1);

        Delivery(Callable<Receipt> action) {
            super(action);
        }

        @Override
        public void run() {
            try {
                super.run();
            } finally {
                release();
            }
        }

        Receipt await(long nanos) throws InterruptedException, ExecutionException, TimeoutException {
            if (!finished.await(nanos, TimeUnit.NANOSECONDS)) {
                throw new TimeoutException();
            }
            return get();
        }

        void discard() {
            cancel(false);
            release();
        }

        private void release() {
            // Cancellation can complete the FutureTask before producer.send actually returns.
            inFlight.set(false);
            finished.countDown();
        }
    }
}
