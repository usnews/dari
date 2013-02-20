package com.psddev.dari.util;

import java.io.IOException;

/** Thrown when anything's wrong authenticating a user. */
@SuppressWarnings("serial")
public class AuthenticationException extends Exception implements HtmlObject {

    public AuthenticationException() {
        super();
    }

    public AuthenticationException(String message) {
        super(message);
    }

    public AuthenticationException(String message, Throwable cause) {
        super(message, cause);
    }

    public AuthenticationException(Throwable cause) {
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
