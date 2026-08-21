package com.dmp.common.id;

import java.security.SecureRandom;
import java.util.UUID;

/**
 * Identifier generation for the platform.
 *
 * <p>All identifiers are UUIDv7 (RFC 9562): a 48-bit big-endian Unix millisecond timestamp
 * followed by randomness. Unlike UUIDv4 this is time-ordered, which matters because the
 * {@code run}, {@code split} and {@code checkpoint} tables grow without bound and are always
 * queried by recency. Random identifiers scatter B-tree inserts across the whole index and
 * destroy cache locality; time-ordered ones append to the right-hand edge.
 *
 * <p>Generation is centralised here so the scheme can change without touching call sites.
 */
public final class Ids {

    private static final SecureRandom RANDOM = new SecureRandom();

    /** Version nibble for UUIDv7, positioned in bits 12-15 of the most significant long. */
    private static final long VERSION_7 = 0x7000L;

    /** IETF variant bits (10xx) in the two most significant bits of the least significant long. */
    private static final long VARIANT_IETF = 0x8000_0000_0000_0000L;
    private static final long VARIANT_MASK = 0x3FFF_FFFF_FFFF_FFFFL;

    private static final int RAND_A_BITS = 0x1000;

    private Ids() {
    }

    /**
     * Generates a time-ordered UUIDv7.
     *
     * <p>Ordering is guaranteed across milliseconds, not within one. Two identifiers minted in
     * the same millisecond may sort arbitrarily relative to each other, which is acceptable
     * because the property being bought here is index locality, not a total order. Anything
     * requiring a strict sequence uses an explicit sequence column instead.
     */
    public static UUID newId() {
        long timestampMillis = System.currentTimeMillis();
        long randA = RANDOM.nextInt(RAND_A_BITS);

        long msb = (timestampMillis << 16) | VERSION_7 | randA;
        long lsb = (RANDOM.nextLong() & VARIANT_MASK) | VARIANT_IETF;

        return new UUID(msb, lsb);
    }

    /**
     * Extracts the creation timestamp from a UUIDv7 as milliseconds since the epoch.
     *
     * @throws IllegalArgumentException if the identifier is not version 7
     */
    public static long timestampOf(UUID id) {
        if (id.version() != 7) {
            throw new IllegalArgumentException(
                    "Not a UUIDv7, cannot extract a timestamp: " + id + " (version " + id.version() + ")");
        }
        return id.getMostSignificantBits() >>> 16;
    }
}
