package org.apache.doris.plugins.openlineage.emit;

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.util.Timeout;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.channels.UnresolvedAddressException;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * One shared client. Retries and redirects are disabled on this instance, not by a JVM property.
 */
public final class LineageHttpPoster implements HttpPoster, AutoCloseable {
    private final CloseableHttpClient client;

    public LineageHttpPoster(Duration connectTimeout) {
        this.client = HttpClients.custom()
                .disableAutomaticRetries()
                .disableRedirectHandling()
                .setDefaultRequestConfig(requestConfig(connectTimeout))
                .build();
    }

    @Override
    public PostResult post(URI url, byte[] body, Map<String, String> headers, Duration timeout) {
        HttpPost request = new HttpPost(url);
        request.setConfig(requestConfig(timeout));
        request.setEntity(new ByteArrayEntity(body, ContentType.APPLICATION_JSON));
        headers.forEach(request::setHeader);
        try (ClassicHttpResponse response = client.execute(request)) {
            String retryAfter = response.getFirstHeader("Retry-After") == null
                    ? null
                    : response.getFirstHeader("Retry-After").getValue();
            return PostResult.http(response.getCode(), EventSender.parseRetryAfter(retryAfter, ZonedDateTime.now(ZoneOffset.UTC)));
        } catch (Exception e) {
            return classify(e);
        }
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    CloseableHttpClient client() {
        return client;
    }

    static PostResult classify(Throwable error) {
        Throwable outer = unwrapAsync(error);
        if (isTimeout(outer)) {
            return PostResult.failure("timeout");
        }
        if (outer instanceof SSLException) {
            return PostResult.failure("tls");
        }
        if (chainHas(outer, UnknownHostException.class) || chainHas(outer, UnresolvedAddressException.class)) {
            return PostResult.failure("dns");
        }
        if (outer instanceof ConnectException || outer instanceof IOException) {
            return PostResult.failure("connect");
        }
        return PostResult.failure("connect");
    }

    private static boolean isTimeout(Throwable error) {
        String name = error.getClass().getName();
        return error instanceof TimeoutException
                || name.endsWith("TimeoutException")
                || name.endsWith("ConnectTimeoutException")
                || name.endsWith("ConnectionRequestTimeoutException");
    }

    private static boolean chainHas(Throwable error, Class<?> type) {
        for (Throwable current = error; current != null; current = current.getCause()) {
            if (type.isInstance(current)) {
                return true;
            }
        }
        return false;
    }

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

    private static RequestConfig requestConfig(Duration timeout) {
        Timeout limit = Timeout.of(timeout);
        return RequestConfig.custom()
                .setConnectTimeout(limit)
                .setResponseTimeout(limit)
                .setConnectionRequestTimeout(limit)
                .build();
    }
}
