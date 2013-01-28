package com.psddev.dari.util;

import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Array;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Writer implementation that adds basic HTML formatting. */
public class HtmlWriter extends Writer {

    private final Writer writer;
    private final Map<Class<?>, HtmlFormatter<Object>> defaultFormatters = new HashMap<Class<?>, HtmlFormatter<Object>>();
    private final Map<Class<?>, HtmlFormatter<Object>> overrideFormatters = new HashMap<Class<?>, HtmlFormatter<Object>>();
    private final Deque<String> tags = new ArrayDeque<String>();

    /** Creates an instance that writes to the given {@code writer}. */
    public HtmlWriter(Writer writer) {
        this.writer = writer;
    }

    @SuppressWarnings("unchecked")
    public <T> void putDefault(Class<T> objectClass, HtmlFormatter<? super T> formatter) {
        defaultFormatters.put(objectClass, (HtmlFormatter<Object>) formatter);
    }

    @SuppressWarnings("unchecked")
    public <T> void putOverride(Class<T> objectClass, HtmlFormatter<? super T> formatter) {
        overrideFormatters.put(objectClass, (HtmlFormatter<Object>) formatter);
    }

    public void putAllStandardDefaults() {
        putDefault(null, HtmlFormatter.NULL);
        putDefault(Class.class, HtmlFormatter.CLASS);
        putDefault(Collection.class, HtmlFormatter.COLLECTION);
        putDefault(Date.class, HtmlFormatter.DATE);
        putDefault(Double.class, HtmlFormatter.FLOATING_POINT);
        putDefault(Enum.class, HtmlFormatter.ENUM);
        putDefault(Float.class, HtmlFormatter.FLOATING_POINT);
        putDefault(Map.class, HtmlFormatter.MAP);
        putDefault(Number.class, HtmlFormatter.NUMBER);
        putDefault(PaginatedResult.class, HtmlFormatter.PAGINATED_RESULT);
        putDefault(StackTraceElement.class, HtmlFormatter.STACK_TRACE_ELEMENT);
        putDefault(Throwable.class, HtmlFormatter.THROWABLE);

        // Optional.
        if (HtmlFormatter.JASPER_EXCEPTION_CLASS != null) {
            putDefault(HtmlFormatter.JASPER_EXCEPTION_CLASS, HtmlFormatter.JASPER_EXCEPTION);
        }
    }

    public void removeDefault(Class<?> objectClass) {
        defaultFormatters.remove(objectClass);
    }

    public void removeOverride(Class<?> objectClass) {
        overrideFormatters.remove(objectClass);
    }

    /**
     * Escapes the given {@code string} so that it's safe to use in
     * an HTML page.
     */
    protected String escapeHtml(String string) {
        return StringUtils.escapeHtml(string);
    }

    private void writeAttribute(Object name, Object value) throws IOException {
        if (!(ObjectUtils.isBlank(name) || value == null)) {
            writer.write(' ');
            writer.write(escapeHtml(name.toString()));
            writer.write("=\"");
            writer.write(escapeHtml(value.toString()));
            writer.write('"');
        }
    }

    /**
     * Writes the given {@code tag} with the given {@code attributes}.
     *
     * <p>This method doesn't keep state, so it should be used with doctype
     * declaration and self-closing tags like {@code img}.</p>
     */
    public HtmlWriter tag(String tag, Object... attributes) throws IOException {
        if (tag == null) {
            throw new IllegalArgumentException("Tag can't be null!");
        }

        writer.write('<');
        writer.write(tag);

        if (attributes != null) {
            for (int i = 0, length = attributes.length; i < length; ++ i) {
                Object name = attributes[i];

                if (name instanceof Map) {
                    for (Map.Entry<?, ?> entry : ((Map<?, ?>) name).entrySet()) {
                        writeAttribute(entry.getKey(), entry.getValue());
                    }

                } else {
                    ++ i;
                    Object value = i < length ? attributes[i] : null;
                    writeAttribute(name, value);
                }
            }
        }

        writer.write('>');
        return this;
    }

    /**
     * Writes the given start {@code tag} with the given {@code attributes}.
     *
     * <p>This method keeps state, so there should be a matching {@link #end}
     * call afterwards.</p>
     */
    public HtmlWriter start(String tag, Object... attributes) throws IOException {
        tag(tag, attributes);
        tags.addFirst(tag);
        return this;
    }

    /** Writes the end tag previously started with {@link #start}. */
    public HtmlWriter end() throws IOException {
        String tag = tags.removeFirst();

        if (tag == null) {
            throw new IllegalStateException("No more tags!");
        }

        writer.write("</");
        writer.write(tag);
        writer.write('>');

        return this;
    }

    /**
     * Escapes and writes the given {@code unescapedHtml}, or if it's
     * {@code null}, the given {@code defaultUnescapedHtml}.
     */
    public HtmlWriter htmlOrDefault(Object unescapedHtml, String defaultUnescapedHtml) throws IOException {
        writer.write(escapeHtml(unescapedHtml == null ? defaultUnescapedHtml : unescapedHtml.toString()));
        return this;
    }

    /**
     * Escapes and writes the given {@code unescapedHtml}, or if it's
     * {@code null}, nothing.
     */
    public HtmlWriter html(Object unescapedHtml) throws IOException {
        htmlOrDefault(unescapedHtml, "");
        return this;
    }

    /** Formats and writes the given {@code object}. */
    public HtmlWriter object(Object object) throws IOException {
        HtmlFormatter<Object> formatter;

        if (object == null) {
            formatter = overrideFormatters.get(null);
            if (formatter == null) {
                formatter = defaultFormatters.get(null);
            }
            if (formatter != null) {
                formatter.format(this, null);
                return this;
            }

        } else {
            if (formatWithMap(overrideFormatters, object)) {
                return this;
            }

            if (object instanceof HtmlObject) {
                ((HtmlObject) object).format(this);
                return this;
            }

            if (formatWithMap(defaultFormatters, object)) {
                return this;
            }
        }

        if (object != null && object.getClass().isArray()) {
            start("ul");
                for (int i = 0, length = Array.getLength(object); i < length; ++ i) {
                    start("li").object(Array.get(object, i)).end();
                }
            end();
            return this;
        }

        return html(object);
    }

    private boolean formatWithMap(
            Map<Class<?>, HtmlFormatter<Object>> formatters,
            Object object)
            throws IOException {

        HtmlFormatter<Object> formatter;

        for (Class<?> objectClass = object.getClass();
                objectClass != null;
                objectClass = objectClass.getSuperclass()) {

            formatter = formatters.get(objectClass);
            if (formatter != null) {
                formatter.format(this, object);
                return true;
            }

            for (Class<?> interfaceClass : objectClass.getInterfaces()) {
                formatter = formatters.get(interfaceClass);
                if (formatter != null) {
                    formatter.format(this, object);
                    return true;
                }
            }
        }

        return false;
    }

    /** Returns a CSS string based on the given {@code properties}. */
    public String cssString(Object... properties) {
        return Static.cssString(properties);
    }

    /**
     * Writes the given {@code object} and positions it according to the
     * given {@code grid}.
     *
     * @see <a href="http://dev.w3.org/csswg/css3-grid-layout/">CSS Grid Layout</a>
     */
    public HtmlWriter grid(Object object, HtmlGrid grid) throws IOException {
        Map<String, Area> areas = createAreas(grid);

        if (areas == null || areas.isEmpty()) {
            if (object instanceof Map) {
                object = ((Map<?, ?>) object).values();
            }

            if (object instanceof Iterable) {
                for (Object item : (Iterable<?>) object) {
                    object(item);
                }

            } else {
                object(object);
            }

        } else {
            if (object == null) {
                object = areas;
            }

            start("div", "style", cssString(
                    "float", "left",
                    "width", "100%"));

                for (Map.Entry<String, Area> entry : areas.entrySet()) {
                    String name = entry.getKey();
                    Area area = entry.getValue();
                    Object value = CollectionUtils.getByPath(object, name);

                    write(area.htmlBefore);
                        start("div",
                                "class", "cms-grid-area",
                                "data-grid-area", name);
                            object(value);
                        end();
                    write(area.htmlAfter);
                }

            end();

            start("div", "style", cssString("clear", "left"));
            end();
        }

        return this;
    }

    private Map<String, Area> createAreas(HtmlGrid grid) {
        if (grid == null) {
            return null;
        }

        List<CssUnit> columns = grid.getColumns();
        List<CssUnit> rows = grid.getRows();
        List<List<String>> template = grid.getTemplate();
        Map<String, Area> areaInstances = new LinkedHashMap<String, Area>();
        int clearAt = -1;

        for (int rowStart = 0, rowSize = rows.size(); rowStart < rowSize; ++ rowStart) {
            List<String> areas = template.get(rowStart);

            for (int columnStart = 0, columnSize = columns.size(); columnStart < columnSize; ++ columnStart) {
                String area = areas.get(columnStart);

                if (!(area == null || ".".equals(area))) {
                    int rowStop = rowStart + 1;
                    int columnStop = columnStart + 1;

                    for (; columnStop < columnSize; ++ columnStop) {
                        if (!ObjectUtils.equals(areas.get(columnStop), area)) {
                            break;
                        } else {
                            areas.set(columnStop, null);
                        }
                    }

                    for (; rowStop < rowSize; ++ rowStop) {
                        if (columnStart < template.get(rowStop).size() && !ObjectUtils.equals(template.get(rowStop).get(columnStart), area)) {
                            break;
                        } else {
                            for (int i = columnStart; i < columnStop; ++ i) {
                                if (i < template.get(rowStop).size()) {
                                    template.get(rowStop).set(i, null);
                                }
                            }
                        }
                    }

                    StringBuilder htmlBefore = new StringBuilder();
                    StringBuilder htmlAfter = new StringBuilder();

                    double frMax;
                    double frBefore;
                    double frAfter;
                    double frBeforeRatio;
                    double frAfterRatio;

                    frMax = 0;
                    frBefore = 0;
                    frAfter = 0;

                    for (int i = 0; i < columnSize; ++ i) {
                        CssUnit column = columns.get(i);

                        if ("fr".equals(column.getUnit())) {
                            double fr = column.getNumber();

                            frMax += fr;

                            if (i < columnStart) {
                                frBefore += fr;

                            } else if (i >= columnStop) {
                                frAfter += fr;
                            }
                        }
                    }

                    frBeforeRatio = frMax > 0 ? frBefore / frMax : 0.0;
                    frAfterRatio = frMax > 0 ? frAfter / frMax : 0.0;

                    htmlBefore.append("<div style=\"margin-left:-30000px;margin-right:30000px;\">");
                    htmlAfter.insert(0, "</div>");

                    htmlBefore.append("<div style=\"float:left;margin:0 -100% 0 ");
                    htmlBefore.append(frBeforeRatio * 100.0);
                    htmlBefore.append("%;width:");
                    htmlBefore.append(frMax > 0 ? ((frMax - frBefore - frAfter) * 100.0 / frMax) + "%" : "auto");
                    htmlBefore.append(";\">");
                    htmlAfter.insert(0, "</div>");

                    Map<String, Adjustment> adjustments = new HashMap<String, Adjustment>();

                    for (int i = 0; i < columnSize; ++ i) {
                        CssUnit column = columns.get(i);
                        String columnUnit = column.getUnit();

                        if (!"fr".equals(columnUnit)) {
                            double columnNumber = column.getNumber();
                            double left = columnNumber * ((i < columnStart ? 1 : 0) - frBeforeRatio);
                            double right = columnNumber * ((i >= columnStop ? 1 : 0) - frAfterRatio);

                            if (left != 0.0 || right != 0.0) {
                                Adjustment adjustment = adjustments.get(columnUnit);

                                if (adjustment == null) {
                                    adjustment = new Adjustment();
                                    adjustments.put(columnUnit, adjustment);
                                }

                                adjustment.left += left;
                                adjustment.right += right;
                            }
                        }
                    }

                    frMax = 0;
                    frBefore = 0;

                    for (int i = rowSize - 1; i >= 0; -- i) {
                        CssUnit row = rows.get(i);

                        if (i < rowStart && "auto".equals(row.getUnit())) {
                            break;

                        } else if ("fr".equals(row.getUnit())) {
                            double fr = row.getNumber();

                            frMax += fr;

                            if (i < rowStart) {
                                frBefore += fr;
                            }
                        }
                    }

                    frBeforeRatio = frMax > 0 ? frBefore / frMax : 0.0;

                    for (int i = rowSize - 1; i >= 0; -- i) {
                        CssUnit row = rows.get(i);
                        String rowUnit = row.getUnit();

                        if (i < rowStart && "auto".equals(rowUnit)) {
                            break;

                        } else if (!"fr".equals(rowUnit)) {
                            double top = row.getNumber() * ((i < rowStart ? 1 : 0) - frBeforeRatio);

                            if (top != 0.0) {
                                Adjustment adjustment = adjustments.get(rowUnit);

                                if (adjustment == null) {
                                    adjustment = new Adjustment();
                                    adjustments.put(rowUnit, adjustment);
                                }

                                adjustment.top += top;
                            }
                        }
                    }

                    for (Map.Entry<String, Adjustment> entry : adjustments.entrySet()) {
                        String unit = entry.getKey();
                        Adjustment adjustment = entry.getValue();

                        htmlBefore.append("<div style=\"margin:");
                        htmlBefore.append(new CssUnit(adjustment.top - 1, unit));
                        htmlBefore.append(" ");
                        htmlBefore.append(new CssUnit(adjustment.right - 1, unit));
                        htmlBefore.append(" -1px ");
                        htmlBefore.append(new CssUnit(adjustment.left - 1, unit));
                        htmlBefore.append(";padding:");
                        htmlBefore.append(new CssUnit(1, unit));
                        htmlBefore.append(" ");
                        htmlBefore.append(new CssUnit(1, unit));
                        htmlBefore.append(" 1px ");
                        htmlBefore.append(new CssUnit(1, unit));
                        htmlBefore.append(";\">");
                        htmlAfter.insert(0, "</div>");
                    }

                    htmlBefore.append("<div style=\"margin-left:30000px;margin-right:-30000px;\">");
                    htmlAfter.insert(0, "</div>");

                    // Area size.
                    boolean autoHeight = false;

                    for (int i = rowStart; i < rowStop; ++ i) {
                        if ("auto".equals(rows.get(i).getUnit())) {
                            autoHeight = true;
                            break;
                        }
                    }

                    htmlBefore.append("<div class=\"cms-grid-areaWrapper\" style=\"height:100%;width:100%;\">");

                    // Minimum width.
                    for (int i = columnStart; i < columnStop; ++ i) {
                        CssUnit column = columns.get(i);

                        if (!"fr".equals(column.getUnit())) {
                            htmlBefore.append("<div style=\"padding-left:");
                            htmlBefore.append(column);
                            htmlBefore.append(";height:0;\">");
                        }
                    }

                    for (int i = columnStart; i < columnStop; ++ i) {
                        if (!"fr".equals(columns.get(i).getUnit())) {
                            htmlBefore.append("</div>");
                        }
                    }

                    // Minimum height.
                    if (!autoHeight) {
                        htmlBefore.append("<div style=\"float:left;width:0;\">");

                        for (int i = rowStart; i < rowStop; ++ i) {
                            CssUnit row = rows.get(i);

                            htmlBefore.append("<div style=\"padding-top:");
                            htmlBefore.append(row);
                            htmlBefore.append(";width:0;\">");
                        }

                        for (int i = rowStart; i < rowStop; ++ i) {
                            htmlBefore.append("</div>");
                        }

                        htmlBefore.append("</div>");
                        htmlBefore.append("<div style=\"height:0;width:100%;\">");

                        htmlAfter.insert(0, "</div>");
                        htmlAfter.insert(0, "<div style=\"clear:left;\"></div>");
                    }

                    htmlAfter.insert(0, "</div>");

                    if (clearAt >= 0 && clearAt <= rowStart) {
                        clearAt = -1;
                        htmlBefore.insert(0, "<div style=\"clear:left;\"></div>");
                    }

                    if (autoHeight && rowStop > clearAt) {
                        clearAt = rowStop;
                    }

                    areaInstances.put(area, new Area(
                            area,
                            htmlBefore.toString(),
                            htmlAfter.toString()));
                }
            }
        }

        return areaInstances;
    }

    /**
     * Writes the given {@code object} and positions it according to the
     * grid rules as specified by the given parameters.
     *
     * @see #grid(Object, HtmlGrid)
     */
    public HtmlWriter grid(Object object, String columns, String rows, String... template) throws IOException {
        return grid(object, new HtmlGrid(columns, rows, template));
    }

    public static class Area {

        private final String name;
        protected final String htmlBefore;
        protected final String htmlAfter;

        public Area(String name, String htmlBefore, String htmlAfter) {
            this.name = name;
            this.htmlBefore = htmlBefore;
            this.htmlAfter = htmlAfter;
        }

        public String getName() {
            return name;
        }
    }

    private static class Adjustment {

        public double left;
        public double right;
        public double top;
    }

    // --- Writer support ---

    @Override
    public Writer append(char letter) throws IOException {
        writer.write(letter);
        return this;
    }

    @Override
    public Writer append(CharSequence text) throws IOException {
        writer.append(text);
        return this;
    }

    @Override
    public Writer append(CharSequence text, int start, int end) throws IOException {
        writer.append(text, start, end);
        return this;
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }

    @Override
    public void flush() throws IOException {
        writer.flush();
    }

    @Override
    public void write(char[] buffer) throws IOException {
        writer.write(buffer);
    }

    @Override
    public void write(char[] buffer, int offset, int length) throws IOException {
        writer.write(buffer, offset, length);
    }

    @Override
    public void write(int letter) throws IOException {
        writer.write(letter);
    }

    @Override
    public void write(String text) throws IOException {
        writer.write(text);
    }

    @Override
    public void write(String text, int offset, int length) throws IOException {
        writer.write(text, offset, length);
    }

    /** {@link HtmlWriter} utility methods. */
    public static final class Static {

        /** Returns a CSS string based on the given {@code properties}. */
        public static String cssString(Object... properties) {
            StringBuilder css = new StringBuilder();

            if (properties != null) {
                for (int i = 1, length = properties.length; i < length; i += 2) {
                    Object property = properties[i - 1];

                    if (property != null) {
                        Object value = properties[i];

                        if (value != null) {
                            css.append(property);
                            css.append(':');
                            css.append(value);
                            css.append(';');
                        }
                    }
                }
            }

            return css.toString();
        }
    }

    // --- Deprecated ---

    /** @deprecated Use {@link #htmlOrDefault} instead. */
    @Deprecated
    public HtmlWriter stringOrDefault(Object string, String defaultString) throws IOException {
        return htmlOrDefault(string, defaultString);
    }

    /** @deprecated Use {@link #html} instead. */
    @Deprecated
    public HtmlWriter string(Object string) throws IOException {
        return html(string);
    }
}
