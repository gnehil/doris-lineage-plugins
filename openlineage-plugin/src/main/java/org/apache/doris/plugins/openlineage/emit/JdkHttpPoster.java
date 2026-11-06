package org.apache.doris.plugins.openlineage.emit;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * One HTTP attempt. Redirects and client retries are off; {@link EventSender} owns the budget.
 */
public final class JdkHttpPoster implements HttpPoster {
    @Override
    public PostResult post(URI url, byte[] body, Map<String, String> headers, Duration timeout) {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(timeout)
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
        HttpRequest.Builder request = HttpRequest.newBuilder(url)
                .timeout(timeout)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        headers.forEach(request::header);
        try {
            HttpResponse<Void> response = client.sendAsync(request.build(), HttpResponse.BodyHandlers.discarding())
                    .orTimeout(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)
                    .join();
            return PostResult.http(response.statusCode(), EventSender.parseRetryAfter(
                    response.headers().firstValue("Retry-After").orElse(null),
                    ZonedDateTime.now(ZoneOffset.UTC)));
        } catch (Exception e) {
            Throwable cause = unwrap(e);
            if (cause instanceof TimeoutException || cause instanceof HttpTimeoutException) {
                return PostResult.failure("timeout");
            }
            if (cause instanceof UnknownHostException) {
                return PostResult.failure("dns");
            }
            if (cause instanceof SSLException) {
                return PostResult.failure("tls");
            }
            if (cause instanceof ConnectException || cause instanceof IOException) {
                return PostResult.failure("connect");
            }
            return PostResult.failure("connect");
        }
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }
}
