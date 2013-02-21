package com.psddev.dari.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.ServletContext;

public class HtmlGrid {

    private final List<CssUnit> columns;
    private final List<CssUnit> rows;
    private final List<List<String>> template;

    public HtmlGrid(String columnsString, String rowsString, String... templateStrings) {
        columns = createCssUnits(columnsString);
        rows = createCssUnits(rowsString);
        template = new ArrayList<List<String>>();

        for (String t : templateStrings) {
            if (t != null && t.length() > 0) {
                for (String line : t.split("[\\r\\n]+")) {
                    line = line.trim();

                    if (line.length() > 0) {
                        List<String> words = Arrays.asList(line.split("\\s+"));
                        int wordsSize = words.size();

                        if (wordsSize > 0) {
                            int lastIndex = wordsSize - 1;
                            String lastWord = words.get(lastIndex);

                            if (lastWord.startsWith("/")) {
                                rows.add(new CssUnit(lastWord.substring(1)));
                                words.remove(lastIndex);
                            }
                        }

                        if (words.size() != columns.size()) {
                            throw new IllegalArgumentException("Columns mismatch!");
                        }

                        template.add(words);
                    }
                }
            }
        }

        if (template.size() != rows.size()) {
            throw new IllegalArgumentException("Rows mismatch!");
        }

    }

    private List<CssUnit> createCssUnits(String values) {
        List<CssUnit> instances = new ArrayList<CssUnit>();

        if (values != null) {
            for (String value : values.trim().split("\\s+")) {
                instances.add(new CssUnit(value));
            }
        }

        return instances;
    }

    public List<CssUnit> getColumns() {
        return columns;
    }

    public List<CssUnit> getRows() {
        return rows;
    }

    public List<List<String>> getTemplate() {
        return template;
    }

    /** Returns all CSS units used by this template. */
    public Set<String> getCssUnits() {
        Set<String> units = new HashSet<String>();

        for (CssUnit column : getColumns()) {
            units.add(column.getUnit());
        }

        for (CssUnit row : getRows()) {
            units.add(row.getUnit());
        }

        return units;
    }

    /** Returns all area names used by this template. */
    public Set<String> getAreas() {
        Set<String> areas = new HashSet<String>();

        for (List<String> row : getTemplate()) {
            for (String area : row) {
                if (!".".equals(area)) {
                    areas.add(area);
                }
            }
        }

        return areas;
    }

    public static final class Static {

        public static HtmlGrid find(ServletContext context, String cssClass) throws IOException {
            return ObjectUtils.isBlank(cssClass) ? null : findGrid(context, "." + cssClass, "/");
        }

        private static HtmlGrid findGrid(ServletContext context, String selector, String path) throws IOException {
            @SuppressWarnings("unchecked")
            Set<String> children = (Set<String>) context.getResourcePaths(path);

            if (children != null) {
                for (String child : children) {
                    if (child.endsWith(".css")) {
                        InputStream cssInput = context.getResourceAsStream(child);

                        try {
                            Css css = new Css(IoUtils.toString(cssInput, StringUtils.UTF_8));

                            if ("grid".equals(css.getValue(selector, "display"))) {
                                String templateValue = css.getValue(selector, "grid-template");

                                if (templateValue != null) {
                                    char[] letters = templateValue.toCharArray();
                                    StringBuilder word = new StringBuilder();
                                    List<String> list = new ArrayList<String>();

                                    for (int i = 0, length = letters.length; i < length; ++ i) {
                                        char letter = letters[i];

                                        if (letter == '"') {
                                            for (++ i; i < length; ++ i) {
                                                letter = letters[i];

                                                if (letter == '"') {
                                                    list.add(word.toString());
                                                    word.setLength(0);
                                                    break;

                                                } else {
                                                    word.append(letter);
                                                }
                                            }

                                        } else if (Character.isWhitespace(letter)) {
                                            if (word.length() > 0) {
                                                list.add(word.toString());
                                                word.setLength(0);
                                            }

                                        } else {
                                            word.append(letter);
                                        }
                                    }

                                    StringBuilder t = new StringBuilder();

                                    for (String v : list) {
                                        t.append(v);
                                        t.append("\n");
                                    }

                                    return new HtmlGrid(
                                            css.getValue(selector, "grid-definition-columns"),
                                            css.getValue(selector, "grid-definition-rows"),
                                            t.toString());
                                }
                            }

                        } finally {
                            cssInput.close();
                        }

                    } else if (child.endsWith("/")) {
                        HtmlGrid grid = findGrid(context, selector, child);
                        
                        if (grid != null) {
                            return grid;
                        }
                    }
                }
            }

            return null;
        }
    }
}
