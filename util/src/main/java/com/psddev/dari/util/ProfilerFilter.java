package com.psddev.dari.util;

import com.psddev.dari.util.AbstractFilter;
import com.psddev.dari.util.HtmlObject;
import com.psddev.dari.util.HtmlWriter;
import com.psddev.dari.util.HtmlFormatter;
import com.psddev.dari.util.JspUtils;
import com.psddev.dari.util.Settings;
import com.psddev.dari.util.StringUtils;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Automatically uses {@link Profiler} to keep track of all events
 * that occur during a HTTP request.
 */
public class ProfilerFilter extends AbstractFilter {

    // --- AbstractFilter support ---

    @Override
    protected void doDispatch(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws Exception {

        if (Settings.isDebug()) {
            super.doDispatch(request, response, chain);

        } else {
            chain.doFilter(request, response);
        }
    }

    @Override
    protected void doInclude(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        // Keep track of JSP includes.
        String path = JspUtils.getCurrentServletPath(request);
        PrintWriter writer = response.getWriter();

        writer.flush();

        try {
            Profiler.Static.startThreadEvent("JSP Include", new JspInclude(path));

            writer.write("<span class=\"_profile-jspStart\" data-jsp=\"");
            writer.write(StringUtils.escapeHtml(path));
            writer.write("\" style=\"display: none;\"></span>");

            chain.doFilter(request, response);

        } finally {
            Profiler.Static.stopThreadEvent();

            writer.write("<span class=\"_profile-jspStop\" data-jsp=\"");
            writer.write(StringUtils.escapeHtml(path));
            writer.write("\" style=\"display: none;\"></span>");
        }
    }

    @Override
    protected void doForward(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        doRequest(request, response, chain);
    }

    @Override
    protected void doRequest(
            final HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        Map<Profiler.Event, Integer> eventIndexes = new HashMap<Profiler.Event, Integer>();
        Profiler profiler = new MarkingProfiler(response, eventIndexes);
        long start = System.nanoTime();

        try {
            Profiler.Static.setThreadProfiler(profiler);
            profiler.startEvent("Request", request.getServletPath());

            chain.doFilter(request, response);

        } finally {
            profiler.stopEvent();
            Profiler.Static.setThreadProfiler(null);

            PrintWriter writer = response.getWriter();

            writer.flush();
            writeResult(Static.getResultWriter(request, response), eventIndexes, profiler, start);
            writer.flush();
        }
    }

    private void writeResult(
            HtmlWriter writer,
            Map<Profiler.Event, Integer> eventIndexes,
            Profiler profiler,
            long start)
            throws IOException {

        Map<String, String> nameColors = new HashMap<String, String>();
        Map<String, String> nameClasses = new HashMap<String, String>();
        double goldenRatio = 0.618033988749895;
        double hue = Math.random();
        int index = 1;

        for (String name : profiler.getEventStats().keySet()) {
            hue += goldenRatio;
            hue %= 1.0;
            nameColors.put(name, "hsl(" + (hue * 360) + ",50%,50%)");
            nameClasses.put(name, "event" + index);
            ++ index;
        }

        writer.start("div", "id", "_profile-result");

            writer.start("div", "class", "navbar navbar-fixed-top");
                writer.start("div", "class", "navbar-inner");
                    writer.start("div", "class", "container-fluid");
                        writer.start("span", "class", "brand").html("Dari \u2192 Profile Result").end();
                    writer.end();
                writer.end();
            writer.end();

            writer.start("div", "class", "container-fluid", "style", "padding-top: 54px;");

                writer.start("div", "id", "_profile-overview");
                    writer.start("div", "class", "row");

                        // Overview of event stats.
                        writer.start("div", "class", "span6");
                            writer.start("h2").html("Overview").end();
                            writer.start("table", "class", "table table-condensed");

                                writer.start("thead");
                                    writer.start("tr");
                                        writer.start("th", "colspan", 2).html("Event").end();
                                        writer.start("th").html("Count").end();
                                        writer.start("th").html("Own Total").end();
                                        writer.start("th").html("Own Average").end();
                                    writer.end();
                                writer.end();

                                writer.start("tbody");
                                    for (Map.Entry<String, Profiler.EventStats> entry : profiler.getEventStats().entrySet()) {
                                        String name = entry.getKey();
                                        Profiler.EventStats stats = entry.getValue();
                                        int count = stats.getCount();
                                        double ownDuration = stats.getOwnDuration() / 1e6;

                                        writer.start("tr");
                                            writer.start("td");
                                                writer.tag("input",
                                                        "type", "checkbox",
                                                        "checked", "checked",
                                                        "value", nameClasses.get(name));
                                            writer.end();
                                            writer.start("td");
                                                writer.start("span",
                                                        "class", "label",
                                                        "style", "background: " + nameColors.get(name) + ";");
                                                    writer.html(name);
                                                writer.end();
                                            writer.end();
                                            writer.start("td").object(count).end();
                                            writer.start("td").object(ownDuration).end();
                                            writer.start("td").object(ownDuration / count).end();
                                        writer.end();
                                    }
                                writer.end();

                            writer.end();
                        writer.end();
                    writer.end();
                writer.end();

                // All events.
                writer.start("div", "id", "_profile-eventTimeline");
                    writer.start("h2").html("Event Timeline").end();
                    writer.start("table", "class", "table table-condensed");

                        writer.start("thead");
                            writer.start("tr");
                                writer.start("th").html("#").end();
                                writer.start("th").html("Start").end();
                                writer.start("th").html("Total").end();
                                writer.start("th").html("Own").end();
                                writer.start("th", "class", "objects").html("Objects").end();
                            writer.end();
                        writer.end();

                        writer.start("tbody");
                            for (Profiler.Event rootEvent : profiler.getRootEvents()) {
                                writeEvent(writer, eventIndexes, start, nameColors, nameClasses, 0, rootEvent);
                            }
                        writer.end();

                    writer.end();
                writer.end();

            writer.end();

        writer.end();

        // Script for formatting and adding interactivity
        // to the profile result.
        writer.start("script", "type", "text/javascript");
            writer.write("(function() {");
                writer.write("var profileScript = document.createElement('script');");
                writer.write("profileScript.src = '/_resource/dari/profiler.js';");
                writer.write("document.body.appendChild(profileScript);");
            writer.write("})();");
        writer.end();
    }

    private void writeEvent(
            HtmlWriter writer,
            Map<Profiler.Event, Integer> eventIndexes,
            long start,
            Map<String, String> nameColors,
            Map<String, String> nameClasses,
            int depth,
            Profiler.Event event)
            throws IOException {

        String name = event.getName();

        writer.start("tr", "class", nameClasses.get(name));

            writer.start("td").html(eventIndexes.get(event)).end();
            writer.start("td").object((event.getStart() - start) / 1e6).end();
            writer.start("td").object(event.getTotalDuration() / 1e6).end();
            writer.start("td").object(event.getOwnDuration() / 1e6).end();

            writer.start("td", "class", "objects", "style", "padding-left: " + (depth * 30 + 5) + "px;");

                writer.start("span",
                        "class", "label",
                        "style", "background: " + nameColors.get(name) + ";");
                    writer.html(name);
                writer.end();

                for (Object item : event.getObjects()) {
                    writer.html(" \u2192 ");
                    writer.object(item);
                }

            writer.end();

            for (Profiler.Event child : event.getChildren()) {
                writeEvent(writer, eventIndexes, start, nameColors, nameClasses, depth + 1, child);
            }

        writer.end();
    }

    /** {@link ProfilerFilter} utility methods. */
    public final static class Static {

        private static final String ATTRIBUTE_PREFIX = ProfilerFilter.class.getName() + ".";
        private static final String RESULT_WRITER_ATTRIBUTE = ATTRIBUTE_PREFIX + ".resultWriter";

        private Static() {
        }

        /**
         * Returns the HTML writer used to render the result.
         *
         * @return Never {@code null}.
         */
        public static HtmlWriter getResultWriter(HttpServletRequest request, HttpServletResponse response) throws IOException {
            ErrorUtils.errorIfNull(request, "request");

            HtmlWriter writer = (HtmlWriter) request.getAttribute(RESULT_WRITER_ATTRIBUTE);

            if (writer == null) {
                writer = new HtmlWriter(response.getWriter());
                writer.putAllStandardDefaults();
                writer.putDefault(Collection.class, COLLECTION_FORMATTER);
                writer.putDefault(StackTraceElement.class, STACK_TRACE_ELEMENT_FORMATTER);
                setResultWriter(request, writer);
            }

            return writer;
        }

        /**
         * Sets the HTML writer used to render the result.
         *
         * @param request Can't be {@code null}.
         * @param writer {@code null} unsets the result writer.
         */
        public static void setResultWriter(HttpServletRequest request, HtmlWriter writer) {
            ErrorUtils.errorIfNull(request, "request");

            request.setAttribute(RESULT_WRITER_ATTRIBUTE, writer);
        }
    }

    // Profiler that marks the start of an event in the HTML.
    private static class MarkingProfiler extends Profiler {

        private final HttpServletResponse response;
        private final Map<Event, Integer> eventIndexes;
        private int eventIndex;

        public MarkingProfiler(HttpServletResponse response, Map<Profiler.Event, Integer> eventIndexes) {
            this.response = response;
            this.eventIndexes = eventIndexes;
        }

        @Override
        public Event startEvent(String name, Object... objects) {
            Event event = super.startEvent(name, objects);

            ++ eventIndex;
            eventIndexes.put(event, eventIndex);

            try {
                PrintWriter writer = response.getWriter();

                writer.write("<span class=\"_profile-eventStart\" data-index=\"");
                writer.write(String.valueOf(eventIndex));
                writer.write("\"></span>");

            } catch (IOException ex) {
            }

            return event;
        }
    }

    private static class JspInclude implements HtmlObject {

        private final String path;

        public JspInclude(String path) {
            this.path = path;
        }

        @Override
        public void format(HtmlWriter writer) throws IOException {
            writer.start("a",
                    "target", "_blank",
                    "href", StringUtils.addQueryParameters(
                            "/_debug/code",
                            "action", "edit",
                            "type", "JSP",
                            "servletPath", path));
                writer.html(path);
            writer.end();
        }
    }

    @SuppressWarnings("all")
    private static final HtmlFormatter<Collection> COLLECTION_FORMATTER = new HtmlFormatter<Collection>() {
        @Override
        public void format(HtmlWriter writer, Collection collection) throws IOException {
            writer.html(collection.getClass().getSimpleName());
            writer.html(": [");
            for (Iterator<Object> i = collection.iterator(); i.hasNext(); ) {
                writer.object(i.next());
                if (i.hasNext()) {
                    writer.write(", ");
                }
            }
            writer.html("]");
        }
    };

    private static final HtmlFormatter<StackTraceElement> STACK_TRACE_ELEMENT_FORMATTER = new HtmlFormatter<StackTraceElement>() {
        @Override
        public void format(HtmlWriter writer, StackTraceElement element) throws IOException {
            writer.html("Called from ");
            HtmlFormatter.STACK_TRACE_ELEMENT.format(writer, element);
        }
    };
}
