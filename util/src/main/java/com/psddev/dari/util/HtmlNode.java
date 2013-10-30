package com.psddev.dari.util;

import java.io.IOException;

/**
 * HTML node.
 */
public abstract class HtmlNode {

    /**
     * Writes this node as HTML to the given {@code writer}.
     *
     * @param writer Can't be {@code null}.
     */
    public abstract void writeHtml(HtmlWriter writer) throws IOException;
}
