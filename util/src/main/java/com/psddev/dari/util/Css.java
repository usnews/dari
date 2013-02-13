package com.psddev.dari.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Css {

    private final char[] css;
    private final int cssLength;
    private int cssIndex;

    private final Map<String, List<CssDeclaration>> rules = new HashMap<String, List<CssDeclaration>>();

    public Css(String css) throws IOException {
        this.css = css.toCharArray();
        this.cssLength = css.length();

        while (true) {
            Set<String> selectors = readSelectors();

            if (selectors == null) {
                break;
            }

            List<CssDeclaration> declarations = readDeclarations();

            for (String selector : selectors) {
                List<CssDeclaration> selectorDeclarations = rules.get(selector);

                if (selectorDeclarations == null) {
                    rules.put(selector, new ArrayList<CssDeclaration>(declarations));
                } else {
                    selectorDeclarations.addAll(declarations);
                }
            }
        }
    }

    public String getValue(String selector, String property) {
        List<CssDeclaration> declarations = rules.get(selector);

        if (declarations != null) {
            for (int i = declarations.size() - 1; i >= 0; -- i) {
                CssDeclaration declaration = declarations.get(i);

                if (declaration.getProperty().equals(property)) {
                    return declaration.getValue();
                }
            }
        }

        return null;
    }

    private void readComment() throws IOException {
        for (; cssIndex < cssLength; ++ cssIndex) {
            if (!Character.isWhitespace(css[cssIndex])) {
                break;
            }
        }

        boolean started = false;
        boolean inSingle = false;
        boolean inMulti = false;
        boolean multiEnding = false;

        for (; cssIndex < cssLength; ++ cssIndex) {
            char letter = css[cssIndex];

            if (letter == '/') {
                if (started) {
                    inSingle = true;

                } else if (multiEnding) {
                    break;

                } else {
                    started = true;
                    multiEnding = false;
                }

            } else if (started && letter == '*') {
                if (inMulti) {
                    multiEnding = true;

                } else {
                    inMulti = true;
                }

            } else if (inSingle && (letter == '\r' || letter == '\n')) {
                break;

            } else if (!(inSingle || inMulti)) {
                if (started) {
                    -- cssIndex;
                }

                break;
            }
        }
    }

    private Set<String> readSelectors() throws IOException {
        readComment();

        Set<String> selectors = null;
        StringBuilder selector = new StringBuilder();

        for (; cssIndex < cssLength; ++ cssIndex) {
            char letter = css[cssIndex];
            boolean brace = letter == '{';

            if (brace || letter == ',') {
                if (selectors == null) {
                    selectors = new HashSet<String>();
                }

                selectors.add(selector.toString().trim());
                selector.setLength(0);

                if (brace) {
                    ++ cssIndex;
                    break;

                } else {
                    readComment();
                }

            } else {
                selector.append(letter);
            }
        }

        return selectors;
    }

    private List<CssDeclaration> readDeclarations() throws IOException {
        readComment();

        List<CssDeclaration> declarations = new ArrayList<CssDeclaration>();
        StringBuilder property = new StringBuilder();
        StringBuilder value = new StringBuilder();
        StringBuilder current = property;

        for (; cssIndex < cssLength; ++ cssIndex) {
            char letter = css[cssIndex];

            if (letter == ':') {
                current = value;

                readComment();

            } else if (letter == ';') {
                current = property;
                declarations.add(new CssDeclaration(property.toString().trim(), value.toString().trim()));
                property.setLength(0);
                value.setLength(0);

                readComment();

            } else if (letter == '}') {
                ++ cssIndex;
                break;

            } else {
                current.append(letter);
            }
        }

        return declarations;
    }
}
