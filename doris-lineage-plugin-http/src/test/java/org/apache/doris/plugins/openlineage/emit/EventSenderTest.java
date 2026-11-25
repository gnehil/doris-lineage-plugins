package org.apache.doris.plugins.openlineage.emit;

import org.apache.doris.plugins.openlineage.config.PluginConfig;
import org.apache.doris.plugins.openlineage.convert.EventEncoder.Event;
import org.apache.doris.plugins.openlineage.emit.EventDelivery.Drop;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventSenderTest {
    @Test
    void doesNotSendCompleteWhenStartNeverSucceeds() throws Exception {
        ScriptPoster poster = new ScriptPoster(
                PostResult.http(500, null),
                PostResult.http(500, null),
                PostResult.http(500, null),
                PostResult.http(200, null));
        Drop result = send(poster, 5_000);

        assertNotNull(result);
        assertEquals(3, poster.bodies.size());
        assertTrue(poster.bodies.stream().allMatch(body -> body.equals("START")));
        assertEquals(3, result.receipt().attempts());
        assertEquals(500, result.receipt().status());
        assertEquals("http", result.receipt().errorType());
        assertEquals("run-1", result.runId());
    }

    @Test
    void retriesOnlyCompleteAfterStartSucceeds() throws Exception {
        ScriptPoster poster = new ScriptPoster(
                PostResult.http(200, null),
                PostResult.http(503, Duration.ofMillis(100)),
                PostResult.http(200, null));
        FakeClock clock = new FakeClock();
        send(poster, clock, 5_000);

        assertEquals(List.of("START", "COMPLETE", "COMPLETE"), poster.bodies);
        assertEquals(Duration.ofMillis(100), clock.slept);
    }

    @Test
    void dropsFourXxAndConnectionFailuresWithoutRetry() throws Exception {
        ScriptPoster rejected = new ScriptPoster(PostResult.http(400, null), PostResult.http(200, null));
        Drop rejectedResult = send(rejected, 5_000);
        assertEquals(1, rejected.bodies.size());
        assertEquals(400, rejectedResult.receipt().status());
        assertEquals("START", rejectedResult.eventType());

        ScriptPoster refused = new ScriptPoster(PostResult.failure("connect"), PostResult.http(200, null));
        Drop refusedResult = send(refused, 5_000);
        assertEquals(1, refused.bodies.size());
        assertNull(refusedResult.receipt().status());
        assertEquals("connect", refusedResult.receipt().errorType());
    }

    @Test
    void doesNotFollowRedirectsOrRetryThem() throws Exception {
        ScriptPoster poster = new ScriptPoster(PostResult.http(302, null), PostResult.http(200, null));
        Drop result = send(poster, 5_000);
        assertEquals(1, poster.bodies.size());
        assertEquals(302, result.receipt().status());
    }

    @Test
    void abandonsWhenRetryAfterExceedsTheRemainingBudget() throws Exception {
        ScriptPoster poster = new ScriptPoster(PostResult.http(429, Duration.ofSeconds(30)), PostResult.http(200, null));
        Drop result = send(poster, 5_000);
        assertEquals(1, poster.bodies.size());
        assertNotNull(result);
        assertEquals(429, result.receipt().status());
    }

    @Test
    void stopsBeforeAnAttemptThatWouldExceedTheBudget() throws Exception {
        ScriptPoster poster = new ScriptPoster(PostResult.failure("timeout"), PostResult.failure("timeout"), PostResult.http(200, null));
        poster.elapsed = Duration.ofSeconds(8);
        Drop result = send(poster, 5_000);
        assertEquals(2, poster.bodies.size());
        assertEquals("timeout", result.receipt().errorType());
        assertNull(result.receipt().status());
    }

    @Test
    void sendsTheBearerTokenButDoesNotRecordIt() throws Exception {
        ScriptPoster poster = new ScriptPoster(PostResult.http(401, null));
        PluginConfig config = PluginConfig.parse(new StringReader("""
                url: https://marquez.example/api/v1/lineage
                cluster: warehouse-a
                auth:
                  type: bearer
                  token: secret
                """));
        Drop result = deliver(config, poster, new FakeClock());
        assertEquals("Bearer secret", poster.headers.get(0).get("Authorization"));
        assertFalse(result.toString().contains("secret"));
    }

    private static Drop send(ScriptPoster poster, int timeoutMs) throws Exception {
        return send(poster, new FakeClock(), timeoutMs);
    }

    private static Drop send(ScriptPoster poster, FakeClock clock, int timeoutMs) throws Exception {
        PluginConfig config = PluginConfig.parse(new StringReader("""
                url: https://marquez.example/api/v1/lineage
                timeoutMs: %d
                cluster: warehouse-a
                auth:
                  type: none
                """.formatted(timeoutMs)));
        return deliver(config, poster, clock);
    }

    private static Drop deliver(PluginConfig config, ScriptPoster poster, FakeClock clock) {
        poster.clock = clock;
        List<Event> events = List.of(
                new Event("START", "run-1", "START".getBytes(StandardCharsets.UTF_8)),
                new Event("COMPLETE", "run-1", "COMPLETE".getBytes(StandardCharsets.UTF_8)));
        return EventDelivery.send(events, new EventSender(config, poster, clock::sleep), clock::monotonicNanos);
    }

    private static final class FakeClock {
        private long nanos;
        private Duration slept = Duration.ZERO;

        public long monotonicNanos() {
            return nanos;
        }

        public void sleep(Duration duration) {
            slept = slept.plus(duration);
            nanos += duration.toNanos();
        }
    }

    private static final class ScriptPoster implements HttpPoster {
        private final List<PostResult> script;
        private final List<String> bodies = new ArrayList<>();
        private final List<Map<String, String>> headers = new ArrayList<>();
        private Duration elapsed = Duration.ZERO;
        private FakeClock clock;

        private ScriptPoster(PostResult... script) {
            this.script = List.of(script);
        }

        @Override
        public PostResult post(java.net.URI url, byte[] body, Map<String, String> headers, Duration timeout) {
            if (clock != null) {
                clock.nanos += elapsed.toNanos();
            }
            this.headers.add(headers);
            bodies.add(new String(body, java.nio.charset.StandardCharsets.UTF_8));
            return script.get(bodies.size() - 1);
        }
    }
}
