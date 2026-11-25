package org.apache.doris.plugins.openlineage.kafka;

import org.apache.doris.plugins.openlineage.convert.EventEncoder.Event;
import org.apache.doris.plugins.openlineage.emit.EventDelivery;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class KafkaEventSinkTest {
    private static final List<Event> EVENTS = List.of(event("START"), event("COMPLETE"));

    @Test
    void publishesBothEventsWithOneRunKeyAfterAcknowledgement() {
        var producer = producer(true);
        try (var sink = new KafkaEventSink(producer, "lineage", Duration.ofSeconds(1))) {
            assertNull(EventDelivery.send(EVENTS, sink, System::nanoTime));
            assertEquals(List.of("run-1", "run-1"), producer.history().stream().map(record -> record.key()).toList());
            assertEquals(List.of("START", "COMPLETE"), producer.history().stream()
                    .map(record -> new String(record.value(), StandardCharsets.UTF_8)).toList());
            assertTrue(producer.history().stream().allMatch(record -> record.topic().equals("lineage")));
        }
    }

    @Test
    void timeoutWaitingForStartPreventsComplete() {
        var producer = producer(false);
        try (var sink = new KafkaEventSink(producer, "lineage", Duration.ofMillis(200))) {
            var drop = assertTimeoutPreemptively(Duration.ofSeconds(2),
                    () -> EventDelivery.send(EVENTS, sink, System::nanoTime));
            assertEquals("START", drop.eventType());
            assertEquals("timeout", drop.receipt().errorType());
            assertEquals(1, producer.history().size());
        }
    }

    @Test
    void timeoutIncludesWaitingInsideProducerSend() {
        var exited = new CountDownLatch(1);
        var producer = new MockProducer<String, byte[]>(true, new StringSerializer(), new ByteArraySerializer()) {
            @Override
            public Future<RecordMetadata> send(ProducerRecord<String, byte[]> record) {
                try {
                    new CountDownLatch(1).await();
                    throw new AssertionError("send must be interrupted");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new org.apache.kafka.common.errors.InterruptException(e);
                } finally {
                    exited.countDown();
                }
            }
        };
        try (var sink = new KafkaEventSink(producer, "lineage", Duration.ofMillis(250))) {
            var drop = assertTimeoutPreemptively(Duration.ofSeconds(2),
                    () -> EventDelivery.send(EVENTS, sink, System::nanoTime));
            assertEquals("START", drop.eventType());
            assertEquals("timeout", drop.receipt().errorType());
            assertTimeoutPreemptively(Duration.ofSeconds(2), () -> { exited.await(); });
            assertTrue(producer.history().isEmpty());
        }
    }

    @Test
    void rejectsEventsWhileTheTimedOutSendStillOwnsTheWorkerAndRecoversAfterward() {
        var release = new Semaphore(0);
        var calls = new AtomicInteger();
        var producer = new MockProducer<String, byte[]>(true, new StringSerializer(), new ByteArraySerializer()) {
            @Override
            public Future<RecordMetadata> send(ProducerRecord<String, byte[]> record) {
                if (calls.getAndIncrement() == 0) {
                    release.acquireUninterruptibly();
                    throw new org.apache.kafka.common.errors.TimeoutException("stalled producer");
                }
                return super.send(record);
            }
        };

        try (var sink = new KafkaEventSink(producer, "lineage", Duration.ofMillis(300))) {
            var stalled = EventDelivery.send(EVENTS, sink, System::nanoTime);
            assertEquals("timeout", stalled.receipt().errorType());
            assertEquals(1, stalled.receipt().attempts());

            var rejected = EventDelivery.send(List.of(event("LINEAGE")), sink, System::nanoTime);
            assertEquals("producer_busy", rejected.receipt().errorType());
            assertEquals(0, rejected.receipt().attempts());
            assertEquals(1, calls.get());

            release.release();
            assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
                while (true) {
                    var recovered = EventDelivery.send(EVENTS, sink, System::nanoTime);
                    if (recovered == null) {
                        break;
                    }
                    assertEquals("producer_busy", recovered.receipt().errorType());
                    Thread.sleep(1);
                }
            });
            assertEquals(List.of("START", "COMPLETE"), producer.history().stream()
                    .map(record -> new String(record.value(), StandardCharsets.UTF_8)).toList());
        } finally {
            release.release();
        }
    }

    private static MockProducer<String, byte[]> producer(boolean autoComplete) {
        return new MockProducer<>(autoComplete, new StringSerializer(), new ByteArraySerializer());
    }

    private static Event event(String type) {
        return new Event(type, "run-1", type.getBytes(StandardCharsets.UTF_8));
    }
}
