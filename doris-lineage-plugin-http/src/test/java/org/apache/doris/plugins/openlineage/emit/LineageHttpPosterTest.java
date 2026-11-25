package org.apache.doris.plugins.openlineage.emit;

import org.apache.hc.client5.http.DnsResolver;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLHandshakeException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpConnectTimeoutException;
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
    void doesNotRetryWhenServerClosesConnection() throws Exception {
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
    void fastFailuresAreNotReportedAsHandoffTimeouts() throws Exception {
        java.util.concurrent.atomic.AtomicInteger lookups = new java.util.concurrent.atomic.AtomicInteger();
        DnsResolver resolver = new DnsResolver() {
            @Override
            public InetAddress[] resolve(String host) throws UnknownHostException {
                lookups.incrementAndGet();
                throw new UnknownHostException(host);
            }

            @Override
            public String resolveCanonicalHostname(String host) {
                return host;
            }
        };
        try (LineageHttpPoster poster = new LineageHttpPoster(Duration.ofMillis(200), resolver)) {
            for (int i = 0; i < 2_000; i++) {
                PostResult result = poster.post(URI.create("http://fast.invalid/api/v1/lineage"),
                        "{}".getBytes(), Map.of(), Duration.ofMillis(200));
                assertEquals("dns", result.errorType());
            }
        }
        assertEquals(2_000, lookups.get());
    }

    @Test
    void blockedDnsDoesNotAccumulateAttempts() throws Exception {
        DnsResolver blocked = new DnsResolver() {
            @Override
            public InetAddress[] resolve(String host) throws UnknownHostException {
                try {
                    Thread.sleep(30_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                throw new UnknownHostException(host);
            }

            @Override
            public String resolveCanonicalHostname(String host) {
                return host;
            }
        };
        try (LineageHttpPoster poster = new LineageHttpPoster(Duration.ofMillis(50), blocked)) {
            for (int i = 0; i < 8; i++) {
                PostResult result = poster.post(URI.create("http://blocked.invalid/api/v1/lineage"),
                        "{}".getBytes(), Map.of(), Duration.ofMillis(50));
                assertEquals("timeout", result.errorType());
            }
            assertTrue(poster.inFlight() <= 1, "inFlight=" + poster.inFlight());
        }
    }

    @Test
    void interruptedCallerDoesNotSpinUntilTheDeadline() throws Exception {
        DnsResolver blocked = new DnsResolver() {
            @Override
            public InetAddress[] resolve(String host) throws UnknownHostException {
                try {
                    Thread.sleep(30_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                throw new UnknownHostException(host);
            }

            @Override
            public String resolveCanonicalHostname(String host) {
                return host;
            }
        };
        java.util.concurrent.atomic.AtomicLong elapsedMs = new java.util.concurrent.atomic.AtomicLong();
        java.util.concurrent.atomic.AtomicBoolean stillInterrupted = new java.util.concurrent.atomic.AtomicBoolean();
        try (LineageHttpPoster poster = new LineageHttpPoster(Duration.ofMillis(300), blocked)) {
            Thread occupier = new Thread(() -> poster.post(URI.create("http://blocked.invalid/api"), "{}".getBytes(), Map.of(), Duration.ofMillis(2_000)));
            occupier.start();
            Thread.sleep(50);
            Thread caller = new Thread(() -> {
                Thread.currentThread().interrupt();
                long started = System.nanoTime();
                poster.post(URI.create("http://blocked.invalid/api"), "{}".getBytes(), Map.of(), Duration.ofMillis(300));
                elapsedMs.set((System.nanoTime() - started) / 1_000_000);
                stillInterrupted.set(Thread.currentThread().isInterrupted());
            });
            caller.start();
            caller.join(1_000);
            assertTrue(elapsedMs.get() < 80, "elapsedMs=" + elapsedMs.get());
            assertTrue(stillInterrupted.get());
            occupier.interrupt();
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
