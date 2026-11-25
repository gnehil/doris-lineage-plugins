package org.apache.doris.plugins.openlineage.emit;

import org.apache.hc.client5.http.DnsResolver;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
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
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * One shared client. Retries and redirects are disabled on this instance, not by a JVM property.
 */
public final class LineageHttpPoster implements HttpPoster {
    private final CloseableHttpClient client;
    private final ThreadPoolExecutor attempts;

    public LineageHttpPoster(Duration connectTimeout) {
        this(connectTimeout, null);
    }

    LineageHttpPoster(Duration connectTimeout, DnsResolver dnsResolver) {
        this.attempts = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), task -> {
            Thread thread = new Thread(task, "openlineage-http");
            thread.setDaemon(true);
            return thread;
        }, new ThreadPoolExecutor.AbortPolicy());
        HttpClientBuilder builder = HttpClients.custom()
                .disableAutomaticRetries()
                .disableRedirectHandling()
                .setDefaultRequestConfig(requestConfig(connectTimeout));
        if (dnsResolver != null) {
            builder.setConnectionManager(PoolingHttpClientConnectionManagerBuilder.create()
                    .setDnsResolver(dnsResolver)
                    .build());
        }
        this.client = builder.build();
    }

    @Override
    public PostResult post(URI url, byte[] body, Map<String, String> headers, Duration timeout) {
        HttpPost request = new HttpPost(url);
        request.setConfig(requestConfig(timeout));
        request.setEntity(new ByteArrayEntity(body, ContentType.APPLICATION_JSON));
        headers.forEach(request::setHeader);
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            if (Thread.currentThread().isInterrupted()) {
                return PostResult.failure("timeout");
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return PostResult.failure("timeout");
            }
            Future<PostResult> exchange;
            try {
                exchange = attempts.submit(() -> execute(request));
            } catch (RejectedExecutionException e) {
                if (Thread.currentThread().isInterrupted()) {
                    return PostResult.failure("timeout");
                }
                LockSupport.parkNanos(Math.min(remaining, 1_000_000L));
                if (Thread.currentThread().isInterrupted()) {
                    return PostResult.failure("timeout");
                }
                continue;
            }
            try {
                return exchange.get(remaining, TimeUnit.NANOSECONDS);
            } catch (TimeoutException e) {
                request.cancel();
                exchange.cancel(true);
                return PostResult.failure("timeout");
            } catch (ExecutionException e) {
                request.cancel();
                return classify(e.getCause() == null ? e : e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                request.cancel();
                exchange.cancel(true);
                return PostResult.failure("timeout");
            }
        }
    }

    private PostResult execute(HttpPost request) {
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
        attempts.shutdownNow();
        client.close();
    }

    CloseableHttpClient client() {
        return client;
    }

    int inFlight() {
        return attempts.getActiveCount();
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
