package org.apache.doris.plugins.openlineage.emit;

import java.time.Duration;

public record PostResult(Integer status, String errorType, Duration retryAfter) {
    public static PostResult http(int status, Duration retryAfter) {
        return new PostResult(status, "http", retryAfter);
    }

    public static PostResult failure(String errorType) {
        return new PostResult(null, errorType, null);
    }

    public boolean success() {
        return status != null && status >= 200 && status < 300;
    }
}
