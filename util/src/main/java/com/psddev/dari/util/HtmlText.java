package com.psddev.dari.util;

import java.io.IOException;

/**
 * HTML text.
 */
public class HtmlText extends HtmlNode {

    private String text;

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    @Override
    public void writeHtml(HtmlWriter writer) throws IOException {
        writer.writeHtml(text);
    }
}
