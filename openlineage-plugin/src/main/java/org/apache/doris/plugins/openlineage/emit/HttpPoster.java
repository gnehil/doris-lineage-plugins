package org.apache.doris.plugins.openlineage.emit;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

public interface HttpPoster {
    PostResult post(URI url, byte[] body, Map<String, String> headers, Duration timeout);
}
