package org.apache.doris.plugins.openlineage.emit;

import org.apache.doris.plugins.openlineage.config.PluginConfig;
import org.apache.doris.plugins.openlineage.convert.EventEncoder.Event;

import java.io.IOException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.function.Consumer;

/** HTTP retry policy for a single encoded event within the shared budget. */
public final class EventSender implements EventSink {
    private static final Duration FIRST_BACKOFF = Duration.ofMillis(200);
    private static final Duration SECOND_BACKOFF = Duration.ofMillis(500);
    private static final int MAX_ATTEMPTS = 3;

    private final PluginConfig config;
    private final HttpPoster poster;
    private final Consumer<Duration> sleeper;
    private final Map<String, String> headers;

    public EventSender(PluginConfig config, HttpPoster poster, Consumer<Duration> sleeper) {
        this.config = config;
        this.poster = poster;
        this.sleeper = sleeper;
        this.headers = config.auth().bearer()
                ? Map.of("Content-Type", "application/json", "Authorization", "Bearer " + config.auth().token())
                : Map.of("Content-Type", "application/json");
    }

    @Override
    public Receipt send(Event event, SendBudget budget) {
        PostResult result = PostResult.failure("timeout");
        int attempts = 0;
        while (attempts < MAX_ATTEMPTS) {
            long timeout = Math.min(config.timeoutMs(), budget.remaining().toMillis());
            if (timeout == 0 || Thread.currentThread().isInterrupted()) {
                break;
            }
            result = poster.post(config.url(), event.body(), headers, Duration.ofMillis(timeout));
            attempts++;
            if (result.success()) {
                return Receipt.success(attempts);
            }
            if (!retryable(result) || attempts == MAX_ATTEMPTS) {
                break;
            }
            Duration wait = result.retryAfter() != null ? result.retryAfter()
                    : attempts == 1 ? FIRST_BACKOFF : SECOND_BACKOFF;
            if (wait.compareTo(budget.remaining()) >= 0) {
                break;
            }
            if (!wait.isZero()) {
                sleeper.accept(wait);
            }
        }
        return Receipt.failure(attempts, result.status(), result.errorType());
    }

    @Override
    public void close() throws IOException {
        poster.close();
    }

    private static boolean retryable(PostResult result) {
        return "timeout".equals(result.errorType())
                || result.status() != null && (result.status() == 429 || result.status() >= 500);
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
}
