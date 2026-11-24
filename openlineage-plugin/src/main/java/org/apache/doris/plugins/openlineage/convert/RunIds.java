package org.apache.doris.plugins.openlineage.convert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.UUID;

final class RunIds {
    static final UUID NAMESPACE = UUID.fromString("8f3c2a1b-6e5d-4c7a-9b0e-1d2f3a4b5c6d");
    private static final ObjectMapper JSON = new ObjectMapper();

    private RunIds() {
    }

    static UUID fromQueryId(String queryId, String clusterNamespace, String jobName) {
        try {
            return UUID.fromString(queryId);
        } catch (IllegalArgumentException ignored) {
            return version5(NAMESPACE, nameBytes(clusterNamespace, jobName, queryId));
        }
    }

    static byte[] nameBytes(String clusterNamespace, String jobName, String queryId) {
        try {
            return JSON.writeValueAsString(List.of(clusterNamespace, jobName, queryId)).getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    static UUID version5(UUID namespace, byte[] name) {
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            sha1.update(bytes(namespace));
            sha1.update(name);
            byte[] hash = sha1.digest();
            hash[6] = (byte) ((hash[6] & 0x0f) | 0x50);
            hash[8] = (byte) ((hash[8] & 0x3f) | 0x80);
            ByteBuffer buffer = ByteBuffer.wrap(hash, 0, 16);
            return new UUID(buffer.getLong(), buffer.getLong());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] bytes(UUID uuid) {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        return buffer.array();
    }
}
