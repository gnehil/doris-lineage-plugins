package org.apache.doris.plugins.openlineage.emit;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLHandshakeException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.channels.UnresolvedAddressException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LineageHttpPosterTest {
    @Test
    void keepsTimeoutAndTlsAheadOfDnsCauses() {
        assertEquals("timeout", LineageHttpPoster.classify(new HttpConnectTimeoutException("timed out")).errorType());
        HttpConnectTimeoutException timeoutWithDns = new HttpConnectTimeoutException("timed out");
        timeoutWithDns.initCause(new UnresolvedAddressException());
        assertEquals("timeout", LineageHttpPoster.classify(timeoutWithDns).errorType());
        ConnectException unresolved = new ConnectException("unresolved");
        unresolved.initCause(new UnresolvedAddressException());
        assertEquals("dns", LineageHttpPoster.classify(unresolved).errorType());
        assertEquals("tls", LineageHttpPoster.classify(new SSLHandshakeException("bad cert")).errorType());
        assertEquals("connect", LineageHttpPoster.classify(new ConnectException("refused")).errorType());
    }

    @Test
    void doesNotRetryAfterAnotherJdkClientHasAlreadyRun() throws Exception {
        try (ServerSocket closed = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            int refused = closed.getLocalPort();
            closed.close();
            try {
                HttpClient.newHttpClient().send(
                        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + refused + "/")).timeout(Duration.ofMillis(200)).GET().build(),
                        HttpResponse.BodyHandlers.discarding());
            } catch (Exception ignored) {
                // Prime the JDK client so its process-wide retry flag is already cached.
            }
        }
        try (ServerSocket server = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"))) {
            AtomicInteger accepts = new AtomicInteger();
            Thread listener = new Thread(() -> {
                while (!server.isClosed()) {
                    try (Socket socket = server.accept()) {
                        accepts.incrementAndGet();
                    } catch (Exception ignored) {
                        return;
                    }
                }
            });
            listener.start();
            try (LineageHttpPoster poster = new LineageHttpPoster(Duration.ofMillis(400))) {
                poster.post(URI.create("http://127.0.0.1:" + server.getLocalPort() + "/api/v1/lineage"),
                        "{}".getBytes(), Map.of(), Duration.ofMillis(400));
            }
            Thread.sleep(300);
            server.close();
            listener.join(500);
            assertEquals(1, accepts.get(), "the client opened more than one connection");
        }
    }

    @Test
    void closeStopsTheSharedClient() throws Exception {
        LineageHttpPoster poster = new LineageHttpPoster(Duration.ofMillis(200));
        poster.close();
        assertThrows(IllegalStateException.class, () -> poster.client().execute(new HttpGet("http://127.0.0.1/")));
    }

    @Test
    void unresolvedHostIsDns() throws Exception {
        try (LineageHttpPoster poster = new LineageHttpPoster(Duration.ofSeconds(5))) {
            PostResult result = poster.post(URI.create("http://does-not-exist.invalid/api/v1/lineage"),
                    "{}".getBytes(), Map.of(), Duration.ofSeconds(5));
            assertEquals("dns", result.errorType());
        }
    }

    @Test
    void slowDripCannotExceedTheAttemptDeadline() throws Exception {
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            Thread drip = new Thread(() -> {
                try (Socket socket = server.accept()) {
                    readHeaders(socket.getInputStream());
                    OutputStream out = socket.getOutputStream();
                    out.write("HTTP/1.1 200 OK\r\nContent-Length: 20\r\n\r\n".getBytes());
                    out.flush();
                    for (int i = 0; i < 20; i++) {
                        out.write('x');
                        out.flush();
                        Thread.sleep(90);
                    }
                } catch (Exception ignored) {
                    // Client cancellation closes the socket.
                }
            });
            drip.start();
            try (LineageHttpPoster poster = new LineageHttpPoster(Duration.ofMillis(300))) {
                long started = System.nanoTime();
                PostResult result = poster.post(
                        URI.create("http://127.0.0.1:" + server.getLocalPort() + "/api/v1/lineage"),
                        "{}".getBytes(),
                        Map.of(),
                        Duration.ofMillis(300));
                long elapsedMs = (System.nanoTime() - started) / 1_000_000;
                assertEquals("timeout", result.errorType());
                assertTrue(elapsedMs < 700, "slow response exceeded the attempt deadline, elapsedMs=" + elapsedMs);
            }
            drip.join(1_000);
        }
    }

    @Test
    void timeoutCancelsAStalledResponse() throws Exception {
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            AtomicInteger closedByPeer = new AtomicInteger();
            Thread stalled = new Thread(() -> {
                try (Socket socket = server.accept()) {
                    InputStream in = socket.getInputStream();
                    OutputStream out = socket.getOutputStream();
                    readHeaders(in);
                    out.write("HTTP/1.1 200 OK\r\nContent-Length: 20\r\n\r\n".getBytes());
                    out.flush();
                    while (in.read() >= 0) {
                        // Wait until the client cancels the stalled body.
                    }
                    closedByPeer.set(1);
                } catch (Exception ignored) {
                    closedByPeer.set(1);
                }
            });
            stalled.start();
            try (LineageHttpPoster poster = new LineageHttpPoster(Duration.ofMillis(800))) {
                PostResult result = poster.post(
                        URI.create("http://127.0.0.1:" + server.getLocalPort() + "/api/v1/lineage"),
                        "{}".getBytes(),
                        Map.of("Content-Type", "application/json"),
                        Duration.ofMillis(800));
                assertEquals("timeout", result.errorType());
            }
            stalled.join(1_500);
            assertTrue(!stalled.isAlive() && closedByPeer.get() == 1, "stalled exchange was still open");
        }
    }

    private static void readHeaders(InputStream in) throws Exception {
        int matched = 0;
        while (matched < 4) {
            int value = in.read();
            if (value < 0) {
                return;
            }
            matched = value == "\r\n\r\n".charAt(matched) ? matched + 1 : (value == '\r' ? 1 : 0);
        }
    }
}
