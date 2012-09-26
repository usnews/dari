package com.psddev.dari.util;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Function for {@link HtmlWriter#object formatting an object} to be
 * written to an {@link HtmlWriter}.
 */
public interface HtmlFormatter<T> {

    /**
     * Formats the given {@code objects} and writes the output to the
     * given {@code writer}.
     */
    public void format(HtmlWriter writer, T object) throws IOException;

    // --- Standard formatters ---

    /** Standard HTML object formatter for classes. */
    @SuppressWarnings("all")
    public static final HtmlFormatter<Class> CLASS = new HtmlFormatter<Class>() {

        @Override
        public void format(HtmlWriter writer, Class rawClassInstance) throws IOException {
            Class<?> classInstance = rawClassInstance;
            writer.start("p");
                writer.start("code").html(classInstance).end();
            writer.end();

            Field[] fields = classInstance.getDeclaredFields();
            Arrays.sort(fields, new Comparator<Field>() {
                @Override
                public int compare(Field x, Field y) {
                    return x.getName().compareTo(y.getName());
                }
            });

            Method[] methods = classInstance.getDeclaredMethods();
            Arrays.sort(methods, new Comparator<Method>() {
                @Override
                public int compare(Method x, Method y) {
                    return x.getName().compareTo(y.getName());
                }
            });

            writer.start("ul");
                if (fields.length > 0) {
                    writer.start("li").html("Fields:").start("ul");
                        for (Field field : fields) {
                            writer.start("li").html(field.getName()).end();
                        }
                    writer.end().end();
                }
                if (methods.length > 0) {
                    writer.start("li").html("Methods:").start("ul");
                        for (Method method : methods) {
                            writer.start("li").html(method.getName()).end();
                        }
                    writer.end().end();
                }
            writer.end();
        }
    };

    /** Standard HTML object formatter for collections. */
    @SuppressWarnings("all")
    public static final HtmlFormatter<Collection> COLLECTION = new HtmlFormatter<Collection>() {

        @Override
        public void format(HtmlWriter writer, Collection collection) throws IOException {
            writer.start("p");
                writer.start("code").html(collection.getClass().getName()).end().html(" containing ");
                writer.start("strong").html(collection.size()).end().html(" items:");
            writer.end();

            writer.start(collection instanceof List ? "ol" : "ul");
                for (Object item : collection) {
                    writer.start("li").object(item).end();
                }
            writer.end();
        }
    };

    /** Standard HTML object formatter for dates. */
    public static final HtmlFormatter<Date> DATE = new HtmlFormatter<Date>() {
        @Override
        public void format(HtmlWriter writer, Date date) throws IOException {
            writer.html(date);
        }
    };

    /** Standard HTML object formatter for enums. */
    @SuppressWarnings("all")
    public static final HtmlFormatter<Enum> ENUM = new HtmlFormatter<Enum>() {
        @Override
        public void format(HtmlWriter writer, Enum enumObject) throws IOException {
            writer.start("code");
                writer.html(enumObject.getClass().getName());
                writer.html('.');
                writer.html(((Enum<?>) enumObject).name());
            writer.end();
        }
    };

    /** Standard HTML object formatter for floating point numbers. */
    public static final HtmlFormatter<Number> FLOATING_POINT = new HtmlFormatter<Number>() {
        @Override
        public void format(HtmlWriter writer, Number number) throws IOException {
            writer.start("abbr", "title", number);
                writer.html(String.format("%,.2f", number));
            writer.end();
        }
    };

    /** Standard HTML object formatter for maps. */
    @SuppressWarnings("all")
    public static final HtmlFormatter<Map> MAP = new HtmlFormatter<Map>() {

        @Override
        public void format(HtmlWriter writer, Map map) throws IOException {
            writer.start("p");
                writer.start("code").html(map.getClass().getName()).end().html(" containing ");
                writer.start("strong").html(map.size()).end().html(" items:");
            writer.end();

            writer.start("dl");
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) map).entrySet()) {
                    writer.start("dt").object(entry.getKey()).end();
                    writer.start("dd").object(entry.getValue()).end();
                }
            writer.end();
        }
    };

    /** Standard HTML object formatter for a {@code null}. */
    public static final HtmlFormatter<Void> NULL = new HtmlFormatter<Void>() {
        @Override
        public void format(HtmlWriter writer, Void nullObject) throws IOException {
            writer.start("code").html("null").end();
        }
    };

    /** Standard HTML object formatter for numbers. */
    public static final HtmlFormatter<Number> NUMBER = new HtmlFormatter<Number>() {
        @Override
        public void format(HtmlWriter writer, Number number) throws IOException {
            if (number instanceof BigDecimal) {
                FLOATING_POINT.format(writer, number);
            } else {
                writer.html(String.format("%,d", number));
            }
        }
    };

    /** Standard HTML object formatter for paginated results. */
    @SuppressWarnings("all")
    public static final HtmlFormatter<PaginatedResult> PAGINATED_RESULT = new HtmlFormatter<PaginatedResult>() {

        @Override
        public void format(HtmlWriter writer, PaginatedResult rawResult) throws IOException {
            PaginatedResult<?> result = rawResult;

            writer.start("p");
                writer.start("code").html(result.getClass().getName()).end();
                writer.html(' ');
                writer.start("strong").html(result.getFirstItemIndex()).end();
                writer.html(" to ");
                writer.start("strong").html(result.getLastItemIndex()).end();
                writer.html(" of ");
                writer.start("strong").html(result.getCount()).end();
            writer.end();

            writer.start("ol");
                for (Object item : result.getItems()) {
                    writer.start("li").object(item).end();
                }
            writer.end();
        }
    };

    /** Standard HTML object formatter for stack trace elements. */
    public static final HtmlFormatter<StackTraceElement> STACK_TRACE_ELEMENT = new HtmlFormatter<StackTraceElement>() {

        @Override
        public void format(HtmlWriter writer, StackTraceElement element) throws IOException {
            String className = element.getClassName();
            int lineNumber = element.getLineNumber();
            String cssClass = className != null &&
                    (className.startsWith("com.psddev.dari.util.AbstractFilter") ||
                    className.startsWith("org.apache.catalina.core.ApplicationFilterChain")) ?
                    "muted" : null;

            String jspServletPath = CodeUtils.getJspServletPath(className);
            if (jspServletPath != null) {
                jspServletPath = StringUtils.ensureStart(jspServletPath, "/");
                lineNumber = CodeUtils.getJspLineNumber(className, lineNumber);
                writer.start("a",
                        "class", cssClass,
                        "target", "_blank",
                        "href", StringUtils.addQueryParameters(
                                "/_debug/code",
                                "action", "edit",
                                "type", "JSP",
                                "servletPath", jspServletPath,
                                "line", lineNumber));
                    writer.html(jspServletPath);
                    writer.html(':');
                    writer.html(lineNumber);
                writer.end();

            } else {
                File source = CodeUtils.getSource(className);
                if (source == null) {
                    writer.start("span", "class", cssClass);
                        writer.html(element);
                    writer.end();

                } else {
                    writer.start("a",
                            "class", cssClass,
                            "target", "_blank",
                            "href", StringUtils.addQueryParameters(
                                    "/_debug/code",
                                    "action", "edit",
                                    "file", source,
                                    "line", lineNumber));
                        int dotAt = className.lastIndexOf('.');
                        if (dotAt > -1) {
                            className = className.substring(dotAt + 1);
                        }
                        writer.html(className);
                        writer.html('.');
                        writer.html(element.getMethodName());
                        writer.html(':');
                        writer.html(lineNumber);
                    writer.end();
                }
            }
        }
    };

    /** Standard HTML object formatter for throwables. */
    public static final HtmlFormatter<Throwable> THROWABLE = new HtmlFormatter<Throwable>() {

        @Override
        public void format(HtmlWriter writer, Throwable throwable) throws IOException {
            Throwable cause = throwable.getCause();
            if (cause != null) {
                writer.object(cause);
            }

            writer.start("div", "class", "dari-throwable");
                String message = throwable.getMessage();

                if (ObjectUtils.isBlank(message)) {
                    writer.html(throwable.getClass().getName());

                } else {
                    writer.html(message);
                    writer.html(" (");
                    writer.html(throwable.getClass().getName());
                    writer.html(")");
                }

                writer.start("ul", "class", "dari-stack-trace");
                    for (StackTraceElement element : throwable.getStackTrace()) {
                        writer.start("li").object(element).end();
                    }
                writer.end();
            writer.end();
        }
    };

    // --- Optional ---

    public static final Class<?> JASPER_EXCEPTION_CLASS = ObjectUtils.getClassByName("org.apache.jasper.JasperException");

    public static final HtmlFormatter<Object> JASPER_EXCEPTION = new HtmlFormatter<Object>() {

        private final Pattern ERROR_PATTERN = Pattern.compile("(line: (\\d+) in the jsp(?s:.)*?)(\\2:(?-s:.)*(?m:$))");
        private final Pattern STACK_TRACE_PATTERN = Pattern.compile("\\s*Stacktrace:\\s*$");

        @Override
        public void format(HtmlWriter writer, Object object) throws IOException {
            Throwable throwable = (Throwable) object;
            Throwable cause = throwable.getCause();
            String message = throwable.getMessage();

            if (cause != null) {
                writer.object(cause);
                if (message != null && message.startsWith(cause.getClass().getName())) {
                    return;
                }
            }

            writer.start("div", "class", "dari-throwable");
                message = StringUtils.escapeHtml(message);

                // Highlight the line mentioned in the error message.
                StringBuilder messageBuilder = new StringBuilder();
                Matcher errorMatcher = ERROR_PATTERN.matcher(message);
                int lastEnd = 0;
                for (; errorMatcher.find(); lastEnd = errorMatcher.end()) {
                    messageBuilder.append(message.substring(lastEnd, errorMatcher.start()));
                    messageBuilder.append(errorMatcher.group(1));
                    messageBuilder.append("<span style=\"background: #B94A48;");
                    messageBuilder.append(" color: white;");
                    messageBuilder.append(" padding: 2px 0;");
                    messageBuilder.append(" text-shadow: none;\">");
                    messageBuilder.append(errorMatcher.group(3));
                    messageBuilder.append("</span>");
                }
                messageBuilder.append(message.substring(lastEnd));

                message = messageBuilder.toString();
                message = STACK_TRACE_PATTERN.matcher(message).replaceFirst("");
                writer.write(message);
            writer.end();
        }
    };
}
