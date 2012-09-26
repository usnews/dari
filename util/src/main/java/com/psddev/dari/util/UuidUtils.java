package com.psddev.dari.util;

import java.security.SecureRandom;
import java.util.UUID;

/** {@link UUID} utility methods. */
public final class UuidUtils {

    private UuidUtils() {
    }

    /** Zero-filled byte array that matches the size of an UUID. */
    public static final byte[] ZERO_BYTES = new byte[16];

    /** Zero-filled UUID. */
    public static final UUID ZERO_UUID = new UUID(0L, 0L);

    /** Creates a sequential UUID. */
    public static synchronized UUID createSequentialUuid() {
        long time = System.currentTimeMillis();

        if (time < LAST_TIME) {
            time = LAST_TIME;
        }

        if (time == LAST_TIME) {
            if (SEQUENCE == 0xffff) {
                ++ time;
                LAST_TIME = time;
                SEQUENCE = 0;

            } else {
                ++ SEQUENCE;
            }

        } else {
            LAST_TIME = time;
            SEQUENCE = 0;
        }

        return new UUID(
                (time & 0xffffffffffff0000L) | NODE_HIGH,
                (time << 16) | NODE_LOW | SEQUENCE);

    }

    private static long LAST_TIME = System.currentTimeMillis();
    private static int SEQUENCE;

    private static final long NODE_HIGH;
    private static final long NODE_LOW;

    static {
        SecureRandom random = new SecureRandom();
        NODE_HIGH = 0xd000L | (random.nextLong() & 0xfff);
        NODE_LOW = 0xa000000000000000L| (random.nextLong() & 0x0fffffff00000000L);
    }

    /**
     * Converts the given {@code uuid} into a byte array.
     *
     * @throws UuidFormatException If the given {@code uuid} is {@code null}.
     */
    public static byte[] toBytes(UUID uuid) {
        if (uuid == null) {
            throw new UuidFormatException(
                    "Can't convert a null into a byte array!");
        }

        byte[] bytes = new byte[16];
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();

        for (int i = 0; i < 8; i ++) {
            bytes[i] = (byte) (msb >>> 8 * (7 - i));
        }

        for (int i = 8; i < 16; i ++) {
            bytes[i] = (byte) (lsb >>> 8 * (7 - i));
        }

        return bytes;
    }

    /**
     * Converts the given {@code bytes} into an UUID.
     *
     * @throws UuidFormatException If the given byte array is {@code null}
     * or its length is not 16.
     */
    public static UUID fromBytes(byte[] bytes) {
        if (bytes == null) {
            throw new UuidFormatException(
                    "Can't convert a null into an UUID!");
        }

        if (bytes.length != 16) {
            throw new UuidFormatException(
                    "Can't convert the byte array into an UUID because"
                    + " its length is not 16!");
        }

        long msb = 0;
        long lsb = 0;

        for (int i = 0; i < 8; i ++) {
            msb = (msb << 8) | (bytes[i] & 0xff);
        }

        for (int i = 8; i < 16; i ++) {
            lsb = (lsb << 8) | (bytes[i] & 0xff);
        }

        return new UUID(msb, lsb);
    }

    /**
     * Converts the given {@code string} into an UUID.
     *
     * @param string Unlike {@link UUID#fromString}, dashes may be omitted.
     * @throws UuidFormatException If the given string is {@code null}
     * or isn't a valid UUID.
     */
    public static UUID fromString(String string) {
        if (string == null) {
            throw new UuidFormatException("Can't convert a null into an UUID!");
        }

        PARSE: {
            int length = string.length();

            if (length == 32 || length == 36) {
                char[] letters = string.toCharArray();
                int letterIndex = 0;
                char letter;
                int letterDigit;

                long msb = 0;
                long lsb = 0;

                for (int i = 0; letterIndex < length && i < 16; ++ letterIndex) {
                    letter = letters[letterIndex];

                    if (letter != '-') {
                        letterDigit = Character.digit(letter, 16);

                        if (letterDigit < 0) {
                            break PARSE;
                        }

                        msb = (msb << 4) | letterDigit;
                        ++ i;
                    }
                }

                for (int i = 0; letterIndex < length && i < 16; ++ letterIndex) {
                    letter = letters[letterIndex];

                    if (letter != '-') {
                        letterDigit = Character.digit(letter, 16);

                        if (letterDigit < 0) {
                            break PARSE;
                        }

                        lsb = (lsb << 4) | letterDigit;
                        ++ i;
                    }
                }

                return new UUID(msb, lsb);
            }
        }

        throw new UuidFormatException("[" + string + "] isn't a valid UUID!");
    }

    // --- Deprecated ---

    /**
     * Creates a time-based version 1 UUID.
     *
     * <p>To use this method, it must be able to access a special
     * <a href="http://johannburkard.de/software/uuid/">UUID library</a>.
     * If you use Maven, you should add the following dependency:</p>
     *
     * <blockquote><pre><code data-type="xml">{@literal
     *<dependency>
     *    <groupId>com.eaio.uuid</groupId>
     *    <artifactId>uuid</artifactId>
     *    <version>3.2</version>
     *</dependency>}</code></pre></blockquote>
     *
     * @deprecated Use the UUID library directy.
     */
    @Deprecated
    public static UUID createVersion1Uuid() {
        com.eaio.uuid.UUID uuid = new com.eaio.uuid.UUID();
        return new UUID(uuid.getTime(), uuid.getClockSeqAndNode());
    }
}
