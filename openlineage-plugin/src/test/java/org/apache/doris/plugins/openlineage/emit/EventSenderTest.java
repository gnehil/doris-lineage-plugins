package org.apache.doris.plugins.openlineage.emit;

import org.apache.doris.plugins.openlineage.config.PluginConfig;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
        EventSender.SendResult result = send(poster, 5_000);

        assertFalse(result.sent());
        assertEquals(3, poster.bodies.size());
        assertTrue(poster.bodies.stream().allMatch(body -> body.equals("START")));
        assertEquals(3, result.drop().attempts());
        assertEquals(500, result.drop().httpStatus());
        assertEquals("http", result.drop().errorType());
        assertEquals("q1", result.drop().queryId());
        assertEquals("hive.dw.out", result.drop().targetTable());
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
        EventSender.SendResult rejectedResult = send(rejected, 5_000);
        assertEquals(1, rejected.bodies.size());
        assertEquals(400, rejectedResult.drop().httpStatus());

        ScriptPoster refused = new ScriptPoster(PostResult.failure("connect"), PostResult.http(200, null));
        EventSender.SendResult refusedResult = send(refused, 5_000);
        assertEquals(1, refused.bodies.size());
        assertNull(refusedResult.drop().httpStatus());
        assertEquals("connect", refusedResult.drop().errorType());
    }

    @Test
    void doesNotFollowRedirectsOrRetryThem() throws Exception {
        ScriptPoster poster = new ScriptPoster(PostResult.http(302, null), PostResult.http(200, null));
        EventSender.SendResult result = send(poster, 5_000);
        assertEquals(1, poster.bodies.size());
        assertEquals(302, result.drop().httpStatus());
    }

    @Test
    void abandonsWhenRetryAfterExceedsTheRemainingBudget() throws Exception {
        ScriptPoster poster = new ScriptPoster(PostResult.http(429, Duration.ofSeconds(30)), PostResult.http(200, null));
        EventSender.SendResult result = send(poster, 5_000);
        assertEquals(1, poster.bodies.size());
        assertFalse(result.sent());
        assertEquals(429, result.drop().httpStatus());
    }

    @Test
    void stopsBeforeAnAttemptThatWouldExceedTheBudget() throws Exception {
        ScriptPoster poster = new ScriptPoster(PostResult.failure("timeout"), PostResult.failure("timeout"), PostResult.http(200, null));
        poster.elapsed = Duration.ofSeconds(8);
        EventSender.SendResult result = send(poster, 5_000);
        assertEquals(2, poster.bodies.size());
        assertEquals("timeout", result.drop().errorType());
        assertNull(result.drop().httpStatus());
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
        EventSender.SendResult result = new EventSender(poster, new FakeClock(), new FakeClock())
                .send(config, "q1", "hive.dw.out", "START", "COMPLETE");
        assertEquals("Bearer secret", poster.headers.get(0).get("Authorization"));
        assertFalse(result.drop().toString().contains("secret"));
    }

    private static EventSender.SendResult send(ScriptPoster poster, int timeoutMs) throws Exception {
        return send(poster, new FakeClock(), timeoutMs);
    }

    private static EventSender.SendResult send(ScriptPoster poster, FakeClock clock, int timeoutMs) throws Exception {
        poster.clock = clock;
        PluginConfig config = PluginConfig.parse(new StringReader("""
                url: https://marquez.example/api/v1/lineage
                timeoutMs: %d
                cluster: warehouse-a
                auth:
                  type: none
                """.formatted(timeoutMs)));
        return new EventSender(poster, clock, clock).send(config, "q1", "hive.dw.out", "START", "COMPLETE");
    }

    private static final class FakeClock implements EventSender.Clock, EventSender.Sleeper {
        private long nanos;
        private Duration slept = Duration.ZERO;

        @Override
        public long monotonicNanos() {
            return nanos;
        }

        @Override
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
