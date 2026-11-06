package org.apache.doris.plugins.openlineage.emit;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * One HTTP attempt on a shared client. Redirects and the JDK connect retry are off;
 * {@link EventSender} owns the budget.
 */
public final class JdkHttpPoster implements HttpPoster, AutoCloseable {
    static {
        // Read once when the JDK HTTP implementation loads. Must be set before the first client.
        System.setProperty("jdk.httpclient.disableRetryConnect", "true");
    }

    private final HttpClient client;

    public JdkHttpPoster(Duration connectTimeout) {
        this.client = HttpClient.newBuilder()
                .connectTimeout(connectTimeout)
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
    }

    @Override
    public PostResult post(URI url, byte[] body, Map<String, String> headers, Duration timeout) {
        HttpRequest.Builder request = HttpRequest.newBuilder(url)
                .version(HttpClient.Version.HTTP_1_1)
                .timeout(timeout)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        headers.forEach(request::header);
        var future = client.sendAsync(request.build(), HttpResponse.BodyHandlers.discarding());
        try {
            HttpResponse<Void> response = future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            return PostResult.http(response.statusCode(), EventSender.parseRetryAfter(
                    response.headers().firstValue("Retry-After").orElse(null),
                    ZonedDateTime.now(ZoneOffset.UTC)));
        } catch (TimeoutException e) {
            future.cancel(true);
            return PostResult.failure("timeout");
        } catch (Exception e) {
            future.cancel(true);
            return classify(unwrapAsync(e));
        }
    }

    @Override
    public void close() {
        try {
            client.getClass().getMethod("shutdownNow").invoke(client);
        } catch (ReflectiveOperationException ignored) {
            // Java 17's HttpClient has no shutdown. The instance still outlives individual attempts.
        }
    }

    static PostResult classify(Throwable error) {
        if (error instanceof HttpConnectTimeoutException || error instanceof HttpTimeoutException || error instanceof TimeoutException) {
            return PostResult.failure("timeout");
        }
        if (error instanceof SSLException) {
            return PostResult.failure("tls");
        }
        if (error instanceof UnknownHostException) {
            return PostResult.failure("dns");
        }
        if (error instanceof ConnectException) {
            return PostResult.failure("connect");
        }
        if (error instanceof IOException) {
            return PostResult.failure("connect");
        }
        return PostResult.failure("connect");
    }

    /** Unwrap only async wrappers. Do not peel a timeout or TLS exception down to its cause. */
    static Throwable unwrapAsync(Throwable error) {
        Throwable current = error;
        while (current instanceof CompletionException || current instanceof ExecutionException) {
            if (current.getCause() == null) {
                break;
            }
            current = current.getCause();
        }
        return current;
    }
}
