package com.dmp.engine;

import com.dmp.domain.audit.AuditPolicy;
import com.dmp.domain.audit.RedactionMode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Removes sensitive fields before a record payload is persisted.
 *
 * <p>Applied at the point of writing, never as a later pass over stored data. Record-level audit
 * captures real customer data, and a platform that logs every record verbatim is a data-protection
 * incident with a delay fuse — one that cannot be fixed retroactively, because by the time anyone
 * notices, the unredacted values are already in the database and in its backups.
 */
public final class Redaction {

    private static final String MASK = "***";

    private Redaction() {
    }

    /**
     * Applies the pipeline's redaction policy to a payload.
     *
     * <p>Paths are JSON pointers, so nested fields such as {@code /customer/email} are reachable.
     * A path that does not match anything is silently ignored: a policy naming a field the source
     * stopped producing should not fail a migration.
     */
    public static JsonNode apply(JsonNode payload, AuditPolicy policy) {
        if (payload == null || policy.redactedFields().isEmpty()) {
            return payload;
        }

        JsonNode copy = payload.deepCopy();
        for (String pointer : policy.redactedFields()) {
            redactAt(copy, pointer, policy.redactionMode());
        }
        return copy;
    }

    private static void redactAt(JsonNode root, String pointer, RedactionMode mode) {
        int lastSlash = pointer.lastIndexOf('/');
        String parentPointer = lastSlash <= 0 ? "" : pointer.substring(0, lastSlash);
        String fieldName = pointer.substring(lastSlash + 1);

        JsonNode parent = parentPointer.isEmpty() ? root : root.at(parentPointer);
        if (!(parent instanceof ObjectNode object) || !object.has(fieldName)) {
            return;
        }

        switch (mode) {
            case DROP -> object.remove(fieldName);
            case MASK -> object.put(fieldName, MASK);
            // Hashing preserves the ability to correlate the same value across runs and records —
            // usually the actual investigative need — without retaining the value itself.
            case HASH -> object.put(fieldName, hash(object.get(fieldName).asText()));
        }
    }

    private static String hash(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashed = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            // Truncated: enough to correlate, short enough to read in a console.
            return "sha256:" + HexFormat.of().formatHex(hashed).substring(0, 16);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandated by the JDK. If it is genuinely absent, masking is the only safe
            // fallback — returning the value would defeat the entire point of this class.
            return MASK;
        }
    }
}
