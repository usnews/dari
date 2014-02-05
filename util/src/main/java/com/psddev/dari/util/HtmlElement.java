package com.psddev.dari.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

/**
 * HTML element.
 */
public class HtmlElement extends HtmlNode {

    private static final Set<String> VOID_ELEMENT_NAMES = ImmutableSet.of(
            "area", "base", "br", "col", "command", "embed", "hr", "img",
            "input", "keygen", "link", "meta", "param", "source", "track",
            "wbr");

    private String name;
    private Map<String, String> attributes;
    private List<HtmlNode> children;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return Never {@code null}. Mutable.
     */
    public Map<String, String> getAttributes() {
        if (attributes == null) {
            attributes = new CompactMap<String, String>();
        }
        return attributes;
    }

    /**
     * @param attributes May be {@code null} to clear the map.
     */
    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    /**
     * Adds all given {@code attributes}.
     *
     * @param attributes May be {@code null}.
     */
    public void addAttributes(Object... attributes) {
        if (attributes == null) {
            return;
        }

        for (int i = 0, length = attributes.length; i < length; ++ i) {
            Object name = attributes[i];

            if (name == null) {
                ++ i;

            } else if (name instanceof Map) {
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) name).entrySet()) {
                    Object n = entry.getKey();
                    Object v = entry.getValue();

                    if (n != null && v != null) {
                        getAttributes().put(n.toString(), v.toString());
                    }
                }

            } else {
                ++ i;

                if (i < length) {
                    Object value = attributes[i];

                    if (value != null) {
                        getAttributes().put(name.toString(), value.toString());
                    }
                }
            }
        }
    }

    /**
     * Returns {@code true} if this tag contains the given
     * {@code attributes}.
     *
     * @param attributes If {@code null}, always returns {@code true}.
     */
    public boolean hasAttributes(Object... attributes) {
        if (attributes == null) {
            return true;
        }

        int length = attributes.length;

        if (length == 0) {
            return true;
        }

        for (int i = 0; i < length; ++ i) {
            Object name = attributes[i];

            if (name == null) {
                throw new IllegalArgumentException();

            } else if (name instanceof Map) {
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) name).entrySet()) {
                    Object n = entry.getKey();

                    if (n == null) {
                        throw new IllegalArgumentException();
                    }

                    Object v = entry.getValue();

                    if (v == null) {
                        throw new IllegalArgumentException();
                    }

                    if (!v.toString().equals(getAttributes().get(n.toString()))) {
                        return false;
                    }
                }

            } else {
                ++ i;

                if (i >= length) {
                    throw new IllegalArgumentException();
                }

                Object value = attributes[i];

                if (value == null) {
                    throw new IllegalArgumentException();
                }

                if (!value.toString().equals(getAttributes().get(name.toString()))) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * @return Never {@code null}. Mutable.
     */
    public List<HtmlNode> getChildren() {
        if (children == null) {
            children = new ArrayList<HtmlNode>();
        }
        return children;
    }

    /**
     * @param children May be {@code null} to clear the list.
     */
    public void setChildren(List<HtmlNode> children) {
        this.children = children;
    }

    @Override
    public void writeHtml(HtmlWriter writer) throws IOException {
        String name = getName();
        Map<String, String> attributes = getAttributes();

        writer.writeRaw("\n");

        if (VOID_ELEMENT_NAMES.contains(name)) {
            writer.writeElement(name, attributes);

        } else {
            List<HtmlNode> children = getChildren();

            writer.writeStart(name, attributes);
                if (children != null) {
                    for (HtmlNode child : children) {
                        child.writeHtml(writer);
                    }
                }
            writer.writeEnd();
        }
    }
}
