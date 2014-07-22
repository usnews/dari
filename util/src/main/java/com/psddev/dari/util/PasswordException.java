package com.psddev.dari.util;

import java.io.IOException;

/** Thrown when anything's wrong with a password. */
@SuppressWarnings("serial")
public class PasswordException extends Exception implements HtmlObject {

    public PasswordException() {
        super();
    }

    public PasswordException(String message) {
        super(message);
    }

    public PasswordException(String message, Throwable cause) {
        super(message, cause);
    }

    public PasswordException(Throwable cause) {
        super(cause);
    }

    // --- HtmlObject support ---

    @Override
    public void format(HtmlWriter writer) throws IOException {
        String message = getMessage();

        if (ObjectUtils.isBlank(message)) {
            message = getClass().getName();
        }

        writer.writeStart("div", "class", "error message");
        writer.writeHtml(message);
        writer.writeEnd();
    }
}
