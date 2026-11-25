package org.apache.doris.plugins.openlineage.emit;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Map;

public interface HttpPoster extends Closeable {
    PostResult post(URI url, byte[] body, Map<String, String> headers, Duration timeout);

    @Override
    default void close() throws IOException {
    }
}
