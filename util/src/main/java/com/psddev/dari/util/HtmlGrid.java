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

                        wordsSize = words.size();
                        int columnsSize = columns.size();

                        if (wordsSize != columnsSize) {
                            throw new IllegalArgumentException(String.format(
                                    "Columns mismatch! [%s] items in [%s] but [%s] in [%s]",
                                    wordsSize, line, columnsSize, columnsString));
                        }

                        template.add(words);
                    }
                }
            }
        }

        int templateSize = template.size();
        int rowsSize = rows.size();

        if (templateSize != rowsSize) {
            StringBuilder t = new StringBuilder();

            if (templateStrings != null) {
                for (String templateString : templateStrings) {
                    t.append("\n");
                    t.append(templateString);
                }
            }

            throw new IllegalArgumentException(String.format(
                    "Rows mismatch! [%s] items in [%s] but [%s] in [%s]",
                    templateSize, t, rowsSize, rowsString));
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

    private HtmlGrid(List<CssUnit> columns, List<CssUnit> rows, List<List<String>> template) {
        this.columns = columns;
        this.rows = rows;
        this.template = template;
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

    public List<HtmlGrid> divide() {
        List<HtmlGrid> divided = new ArrayList<HtmlGrid>();
        List<List<String>> template = getTemplate();
        List<CssUnit> columns = getColumns();
        List<CssUnit> rows = getRows();

        int columnSize = columns.size();
        int rowSize = rows.size();

        for (int rowStart = 0, rowStop; rowStart < rowSize; rowStart = rowStop) {
            rowStop = rowStart + 1;

            for (int i = rowStart; i < rowStop; ++ i) {
                for (int columnIndex = 0; columnIndex < columnSize; ++ columnIndex) {
                    String area = template.get(i).get(columnIndex);

                    if (!area.equals(".")) {
                        int j = i + 1;

                        for (; j < rowSize; ++ j) {
                            if (!area.equals(template.get(j).get(columnIndex))) {
                                break;
                            }
                        }

                        if (rowStop < j) {
                            rowStop = j;
                        }
                    }
                }
            }

            divided.add(new HtmlGrid(
                    columns,
                    rows.subList(rowStart, rowStop),
                    template.subList(rowStart, rowStop)));
        }

        return divided;
    }

    public static final class Static {

        private static final String TEMPLATE_PROPERTY = "grid-template";
        private static final String COLUMNS_PROPERTY = "grid-definition-columns";
        private static final String ROWS_PROPERTY = "grid-definition-rows";

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
                                String templateValue = css.getValue(selector, TEMPLATE_PROPERTY);

                                if (templateValue != null) {
                                    String columnsValue = css.getValue(selector, COLUMNS_PROPERTY);

                                    if (ObjectUtils.isBlank(columnsValue)) {
                                        throw new IllegalStateException(String.format(
                                                "Path: %s, Selector: %s, Missing [%s]!",
                                                child, selector, COLUMNS_PROPERTY));
                                    }

                                    String rowsValue = css.getValue(selector, ROWS_PROPERTY);

                                    if (ObjectUtils.isBlank(rowsValue)) {
                                        throw new IllegalStateException(String.format(
                                                "Path: %s, Selector: %s, Missing [%s]!",
                                                child, selector, ROWS_PROPERTY));
                                    }

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

                                    try {
                                        return new HtmlGrid(
                                                columnsValue,
                                                rowsValue,
                                                t.toString());

                                    } catch (IllegalArgumentException error) {
                                        throw new IllegalArgumentException(String.format(
                                                "Path: %s, Selector: %s, %s",
                                                child, selector, error.getMessage()));
                                    }
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
