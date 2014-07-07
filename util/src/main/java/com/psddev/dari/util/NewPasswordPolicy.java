package com.psddev.dari.util;

/** Policy for {@link Password} creation. */
public interface NewPasswordPolicy extends SettingsBackedObject {

    /** Setting key for all password policy configurations. */
    public static final String SETTING_PREFIX = "dari/newPasswordPolicy";

    /**
     * Validates the given {@code password} for the given {@code user}.
     *
     * @param user Can be {@code null}.
     * @param password Can be {@code null}.
     * @throws PasswordException If anything's wrong with the given
     * {@code password}.
     */
    public void validate(Object user, String password) throws PasswordException;

    /**
     * {@link NewPasswordPolicy} utility methods.
     *
     * <p>The factory method, {@link #getInstance}, uses {@link Settings}
     * to construct instances.</p>
     */
    public static final class Static {

        private static final PullThroughCache<String, NewPasswordPolicy>
                INSTANCES = new PullThroughCache<String, NewPasswordPolicy>() {

            @Override
            public NewPasswordPolicy produce(String name) {
                return Settings.newInstance(NewPasswordPolicy.class, SETTING_PREFIX + "/" + name);
            }
        };

        /**
         * Returns the password policy associated with the given
         * {@code name}.
         *
         * @param name If blank, returns {@code null}.
         * @return May be {@code null}.
         */
        public static NewPasswordPolicy getInstance(String name) {
            return ObjectUtils.isBlank(name) ? null : INSTANCES.get(name);
        }
    }
}
