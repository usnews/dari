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

            if (selectors.isEmpty()) {
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

    private Set<String> readSelectors() throws IOException {
        Set<String> selectors = new HashSet<String>();
        StringBuilder selector = new StringBuilder();

        for (; cssIndex < cssLength; ++ cssIndex) {
            char letter = css[cssIndex];
            boolean brace = letter == '{';

            if (brace || letter == ',') {
                selectors.add(selector.toString().trim());
                selector.setLength(0);

                if (brace) {
                    ++ cssIndex;
                    break;
                }

            } else {
                selector.append(letter);
            }
        }

        return selectors;
    }

    private List<CssDeclaration> readDeclarations() throws IOException {
        List<CssDeclaration> declarations = new ArrayList<CssDeclaration>();
        StringBuilder property = new StringBuilder();
        StringBuilder value = new StringBuilder();
        StringBuilder current = property;

        for (; cssIndex < cssLength; ++ cssIndex) {
            char letter = css[cssIndex];

            if (letter == ':') {
                current = value;

            } else if (letter == ';') {
                current = property;
                declarations.add(new CssDeclaration(property.toString().trim(), value.toString().trim()));
                property.setLength(0);
                value.setLength(0);

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
