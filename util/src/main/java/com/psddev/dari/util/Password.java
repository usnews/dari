package com.psddev.dari.util;

import java.security.SecureRandom;

/** Password that can be stored securely. */
public final class Password {

    public static final String DEFAULT_ALGORITHM = "SHA-512";

    private static final SecureRandom RANDOM = new SecureRandom();

    private String data;

    /** Creates an instance with the given internal {@code data}. */
    public static Password valueOf(String data) {
        Password password = new Password();
        password.data = data;
        return password;
    }

    /**
     * Creates an instance that hashes the given {@code password} using
     * the given {@code algorithm} and {@code salt}.
     */
    public static Password createCustom(String algorithm, String salt, String password) {
        if (password == null) {
            password = "";
        }
        return valueOf(algorithm + ":" + salt + ":" + hash(algorithm, salt, password));
    }

    /**
     * Creates an instance that hashes the given {@code password} using
     * the the {@value DEFAULT_ALGORITHM} algorithm and a random salt.
     */
    public static Password create(String password) {
        byte[] salt = new byte[8];
        RANDOM.nextBytes(salt);
        return createCustom(DEFAULT_ALGORITHM, StringUtils.hex(salt), password);
    }

    /**
     * Hashes the given {@code algorithm}, {@code salt},
     * and {@code password}.
     */
    private static String hash(String algorithm, String salt, String password) {
        return StringUtils.hex(StringUtils.hash(algorithm, salt + password));
    }

    /** Creates a blank instance. */
    private Password() {
    }

    /**
     * Checks this password against the given {@code string} and returns
     * {@code true} if they're the same.
     */
    public boolean check(String string) {
        String algorithm = "SHA-1";
        String salt = "";
        String hash = data;

        int colon1At = data.indexOf(':');
        if (colon1At >= 0) {
            int colon2At = data.indexOf(':', colon1At + 1);
            if (colon2At >= 0) {
                algorithm = data.substring(0, colon1At);
                salt = data.substring(colon1At + 1, colon2At);
                hash = data.substring(colon2At + 1);
            }
        }

        return hash.equals(hash(algorithm, salt, string));
    }

    // --- Object support ---

    @Override
    public boolean equals(Object other) {
        return this == other
                || (other instanceof Password
                && data.equals(((Password) other).data))
                ;
    }

    @Override
    public int hashCode() {
        return data.hashCode();
    }

    @Override
    public String toString() {
        return data;
    }
}
