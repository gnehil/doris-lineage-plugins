package org.apache.doris.plugins.openlineage.emit;

import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLHandshakeException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpConnectTimeoutException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdkHttpPosterTest {
    @Test
    void keepsTimeoutAndTlsTypesAboveTheirCauses() {
        PostResult classified = JdkHttpPoster.classify(new HttpConnectTimeoutException("timed out"));
        assertEquals("timeout", classified.errorType(), classified.toString());
        assertEquals("timeout", JdkHttpPoster.classify(JdkHttpPoster.unwrapAsync(
                new ExecutionException(new HttpConnectTimeoutException("timed out")))).errorType());
        assertEquals("tls", JdkHttpPoster.classify(new SSLHandshakeException("bad cert")).errorType());
        assertEquals("connect", JdkHttpPoster.classify(new ConnectException("refused")).errorType());
    }

    @Test
    void refusedConnectionIsNotRetriedByTheRealClient() throws Exception {
        try (ServerSocket closed = new ServerSocket(0)) {
            int port = closed.getLocalPort();
            closed.close();
            try (JdkHttpPoster poster = new JdkHttpPoster(Duration.ofMillis(400))) {
                long started = System.nanoTime();
                PostResult result = poster.post(URI.create("http://127.0.0.1:" + port + "/api/v1/lineage"),
                        "{}".getBytes(), Map.of(), Duration.ofMillis(400));
                long elapsedMs = (System.nanoTime() - started) / 1_000_000;
                assertEquals("connect", result.errorType());
                assertTrue(elapsedMs < 700, "refused connect was retried, elapsedMs=" + elapsedMs);
            }
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
            try (JdkHttpPoster poster = new JdkHttpPoster(Duration.ofMillis(800))) {
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
