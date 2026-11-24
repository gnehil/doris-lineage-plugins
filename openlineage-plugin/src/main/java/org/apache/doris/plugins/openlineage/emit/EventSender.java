package org.apache.doris.plugins.openlineage.emit;

import org.apache.doris.plugins.openlineage.config.PluginConfig;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Sends one START and one COMPLETE within a shared deadline. Retries are counted here;
 * the poster must not retry or follow redirects.
 */
public final class EventSender {
    public static final Duration SEND_BUDGET = Duration.ofSeconds(15);
    private static final Duration FIRST_BACKOFF = Duration.ofMillis(200);
    private static final Duration SECOND_BACKOFF = Duration.ofMillis(500);
    private static final int MAX_RETRIES = 2;

    private static final ObjectMapper JSON = new ObjectMapper();

    private final HttpPoster poster;
    private final Clock clock;
    private final Sleeper sleeper;

    public EventSender(HttpPoster poster, Clock clock, Sleeper sleeper) {
        this.poster = poster;
        this.clock = clock;
        this.sleeper = sleeper;
    }

    public SendResult send(PluginConfig config, String queryId, String targetTable, String startJson, String completeJson) {
        long deadline = clock.monotonicNanos() + SEND_BUDGET.toNanos();
        Map<String, String> headers = headers(config);
        Attempt start = sendOne(config.url(), headers, startJson.getBytes(StandardCharsets.UTF_8), config.timeoutMs(), deadline);
        if (!start.success) {
            return SendResult.dropped(drop(queryId, targetTable, "START", startJson, start));
        }
        Attempt complete = sendOne(config.url(), headers, completeJson.getBytes(StandardCharsets.UTF_8), config.timeoutMs(), deadline);
        if (!complete.success) {
            return SendResult.dropped(drop(queryId, targetTable, "COMPLETE", completeJson, complete));
        }
        return SendResult.ok();
    }

    private Attempt sendOne(URI url, Map<String, String> headers, byte[] body, int timeoutMs, long deadline) {
        Integer status = null;
        String errorType = null;
        int attempts = 0;
        while (attempts <= MAX_RETRIES) {
            long remaining = deadline - clock.monotonicNanos();
            if (remaining <= 0) {
                return Attempt.failed(attempts, status, errorType == null ? "timeout" : errorType);
            }
            Duration timeout = Duration.ofMillis(Math.min(timeoutMs, remaining / 1_000_000));
            if (timeout.isZero() || timeout.isNegative()) {
                return Attempt.failed(attempts, status, errorType == null ? "timeout" : errorType);
            }
            PostResult result = poster.post(url, body, headers, timeout);
            attempts++;
            status = result.status();
            errorType = result.errorType();
            if (result.success()) {
                return Attempt.ok(attempts);
            }
            if (!retryable(result) || attempts > MAX_RETRIES) {
                return Attempt.failed(attempts, status, errorType);
            }
            Duration wait = waitFor(result.retryAfter(), attempts);
            if (wait.compareTo(Duration.ofNanos(deadline - clock.monotonicNanos())) > 0) {
                return Attempt.failed(attempts, status, errorType);
            }
            if (!wait.isZero()) {
                sleeper.sleep(wait);
            }
        }
        return Attempt.failed(attempts, status, errorType);
    }

    private static boolean retryable(PostResult result) {
        if ("timeout".equals(result.errorType())) {
            return true;
        }
        return result.status() != null && (result.status() == 429 || result.status() >= 500);
    }

    private static Duration waitFor(Duration retryAfter, int attemptsSoFar) {
        if (retryAfter != null) {
            return retryAfter;
        }
        return attemptsSoFar == 1 ? FIRST_BACKOFF : SECOND_BACKOFF;
    }

    static Duration parseRetryAfter(String header, ZonedDateTime now) {
        if (header == null || header.isBlank()) {
            return null;
        }
        String value = header.trim();
        try {
            long seconds = Long.parseLong(value);
            if (seconds < 0) {
                return null;
            }
            return Duration.ofSeconds(seconds);
        } catch (NumberFormatException ignored) {
            try {
                ZonedDateTime when = ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME);
                Duration wait = Duration.between(now, when);
                return wait.isNegative() ? Duration.ZERO : wait;
            } catch (DateTimeParseException ignoredToo) {
                return null;
            }
        }
    }

    private static Map<String, String> headers(PluginConfig config) {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Content-Type", "application/json");
        if (config.auth().bearer()) {
            headers.put("Authorization", "Bearer " + config.auth().token());
        }
        return headers;
    }

    public interface Clock {
        long monotonicNanos();
    }

    public interface Sleeper {
        void sleep(Duration duration);
    }

    public record Drop(
            String queryId,
            String targetTable,
            String eventType,
            String runId,
            int attempts,
            Integer httpStatus,
            String errorType) {
    }

    private static Drop drop(String queryId, String targetTable, String eventType, String json, Attempt attempt) {
        return new Drop(queryId, targetTable, eventType, runId(json), attempt.attempts, attempt.status, attempt.errorType);
    }

    private static String runId(String json) {
        try {
            JsonNode runId = JSON.readTree(json).path("run").path("runId");
            return runId.isMissingNode() || runId.isNull() ? null : runId.asText();
        } catch (Exception e) {
            return null;
        }
    }

    public record SendResult(boolean sent, Drop drop) {
        static SendResult ok() {
            return new SendResult(true, null);
        }

        static SendResult dropped(Drop drop) {
            return new SendResult(false, drop);
        }
    }

    private record Attempt(boolean success, int attempts, Integer status, String errorType) {
        static Attempt ok(int attempts) {
            return new Attempt(true, attempts, null, null);
        }

        static Attempt failed(int attempts, Integer status, String errorType) {
            return new Attempt(false, attempts, status, errorType);
        }
    }
}
