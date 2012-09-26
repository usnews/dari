package com.psddev.dari.util;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/** Debug servlet that reports application {@link Stats}. */
@DebugFilter.Path("stats")
@SuppressWarnings("serial")
public class StatsDebugServlet extends HttpServlet {

    private enum Type {
        COUNT,
        DURATION
    }

    @Override
    protected void doGet(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        WebPageContext page = new WebPageContext(getServletContext(), request, response);
        Type type = page.param(Type.class, "type");

        if (type != null) {
            String statsName = page.param(String.class, "stats");

            if (statsName != null) {
                for (Stats stats : Stats.Static.getAll()) {
                    if (statsName.equals(stats.getName())) {
                        Stats.Measurement measurement = stats.getMeasurements().get(page.param(String.class, "operation"));

                        if (measurement != null) {
                            response.setContentType("text/javascript");

                            PrintWriter writer = response.getWriter();
                            String callback = page.param(String.class, "callback");

                            if (!ObjectUtils.isBlank(callback)) {
                                writer.write(callback);
                                writer.write("(");
                            }

                            long begin = page.param(long.class, "begin");
                            long end = page.paramOrDefault(long.class, "end", System.currentTimeMillis());

                            writer.write("[");

                            if (Type.COUNT.equals(type)) {
                                for (Iterator<Double> i = measurement.getCountAverages(page.param(int.class, "interval"), begin, end).iterator(); i.hasNext(); ) {
                                    double average = i.next();
                                    writer.write(Double.isNaN(average) ? "null" : String.valueOf(average));
                                    if (i.hasNext()) {
                                        writer.write(",");
                                    }
                                }

                            } else {
                                for (Iterator<Double> i = measurement.getDurationAverages(0, begin, end).iterator(); i.hasNext(); ) {
                                    double average = i.next();
                                    writer.write(Double.isNaN(average) ? "null" : String.valueOf(average * 1e3));
                                    if (i.hasNext()) {
                                        writer.write(",");
                                    }
                                }
                            }

                            writer.write("]");

                            if (!ObjectUtils.isBlank(callback)) {
                                writer.write(");");
                            }

                            return;
                        }
                    }
                }
            }
        }

        new DebugFilter.PageWriter(getServletContext(), request, response) {{
            startPage("Stats");

                start("style", "type", "text/css");
                    write(".chart { float: left; margin: 0 20px 10px 0; }");
                    write("hr { border-color: black; border-top: none; margin-left: -20px; margin-right: -20px; }");

                    write(".axis {");
                        write("font: 10px sans-serif;");
                    write("}");

                    write(".axis text {");
                        write("-webkit-transition: fill-opacity 50ms linear;");
                    write("}");

                    write(".axis path {");
                        write("display: none;");
                    write("}");

                    write(".axis line {");
                        write("stroke: #000;");
                        write("shape-rendering: crispEdges;");
                    write("}");

                    write(".horizon {");
                        write("border-color: black;");
                        write("border-style: solid;");
                        write("border-width: 1px 0;");
                        write("overflow: hidden;");
                        write("position: relative;");
                        write("width: 400px;");
                    write("}");

                    write(".horizon canvas {");
                        write("display: block;");
                    write("}");

                    write(".horizon .title,");
                    write(".horizon .value {");
                        write("bottom: 0;");
                        write("line-height: 30px;");
                        write("margin: 0 6px;");
                        write("position: absolute;");
                        write("text-shadow: 0 1px 0 rgba(255, 255, 255, .5);");
                        write("white-space: nowrap;");
                    write("}");

                    write(".horizon .title {");
                        write("left: 0;");
                        write("top: 0;");
                    write("}");

                    write(".horizon .value {");
                        write("right: 0;");
                    write("}");

                    write(".line {");
                        write("background: #000;");
                        write("opacity: .2;");
                        write("z-index: 2;");
                    write("}");
                end();

                start("script", "type", "text/javascript", "src", "/_resource/d3/d3.v2.min.js").end();
                start("script", "type", "text/javascript", "src", "/_resource/d3/cubism.v1.min.js").end();
                start("script", "type", "text/javascript");
                    write("var maxDataSize = 400;");
                    write("var context = cubism.context().serverDelay(0).clientDelay(0).step(1e3).size(maxDataSize);");
                end();

                for (Iterator<Stats> i = Stats.Static.getAll().iterator(); i.hasNext(); ) {
                    Stats stats = i.next();
                    String statsName = stats.getName();
                    String operation = page.paramOrDefault(String.class, statsName + "/operation", "Total");
                    int intervalIndex = page.param(int.class, statsName + "/interval");

                    start("h2").html(statsName).end();

                    for (Type type : Type.values()) {
                        String divId = JspUtils.createId(page.getRequest());

                        start("div", "class", "chart");
                            start("h3").html(Type.COUNT.equals(type) ? "Throughput (/s)" : "Latency (ms)").end();
                            start("div", "id", divId).end();
                        end();

                        start("script", "type", "text/javascript");
                            write("d3.select('#");
                            write(divId);
                            write("').call(function(div) {");

                                write("div.datum(function() {");
                                    write("var last;");
                                    write("return context.metric(function(begin, end, step, callback) {");
                                        write("begin = +begin, end= +end;");
                                        write("var first = isNaN(last);");
                                        write("if (first) last = begin;");

                                        write("$.getJSON('/_debug/stats', {");
                                            write("'stats': '"); write(page.js(statsName)); write("',");
                                            write("'operation': '"); write(page.js(operation)); write("',");
                                            write("'interval': '"); write(page.js(intervalIndex)); write("',");
                                            write("'type': '"); write(page.js(type)); write("',");
                                            write("'begin': begin,");
                                            write("'end': end");

                                        write("}, function(data) {");
                                            write("if (first) {");
                                                write("var padding = maxDataSize - data.length;");
                                                write("if (padding > 0) {");
                                                    write("var newData = [ ];");
                                                    write("for (var i = 0; i < padding; ++ i) newData.push(0.0);");
                                                    write("data = newData.concat(data);");
                                                write("}");
                                            write("}");
                                            write("callback(null, data);");
                                        write("});");

                                    write("}, '");
                                    write(page.js(operation + " over " + stats.getAverageIntervals().get(intervalIndex).intValue() + "s"));
                                    write("');");
                                write("});");

                                write("div.append('div')");
                                    write(".attr('class', 'axis')");
                                    write(".call(context.axis().orient('top'));");

                                write("div.append('div')");
                                    write(".attr('class', 'horizon')");
                                    write(".call(context.horizon().height(60));");
                            write("});");
                        end();
                    }

                    start("h3", "style", "clear: left;").html("Averages").end();
                    start("table", "class", "table table-condensed");
                        start("thead");
                            start("tr");
                                start("th").html("Operation").end();
                                start("th").html("Total").end();
                                for (double averageInterval : stats.getAverageIntervals()) {
                                    start("th", "colspan", 2).html("Over ").object((int) averageInterval).html("s").end();
                                }
                                start("th", "colspan", 2).html("Over All").end();
                            end();
                        end();
                        start("tbody");
                            for (Map.Entry<String, Stats.Measurement> entry : stats.getMeasurements().entrySet()) {
                                writeStatsMeasurement(stats, entry.getKey(), entry.getValue());
                            }
                        end();
                    end();

                    if (i.hasNext()) {
                        tag("hr");
                    }
                }

                start("script", "type", "text/javascript");
                    write("context.on('focus', function(i) {");
                        write("d3.selectAll('.value').style('right', i == null ? null : context.size() - i + 'px');");
                    write("});");
                end();
            endPage();
        }

            // Writes individual stats measurement.
            private void writeStatsMeasurement(Stats stats, String operation, Stats.Measurement measurement) throws IOException {
                start("tr");
                    start("th").html(operation).end();
                    start("td").object(measurement.getOverallTotalCount()).end();
                    for (int i = 0, size = stats.getAverageIntervals().size(); i < size; ++ i) {
                        writeCountAndDuration(stats, operation, i, measurement.getCurrentCountAverage(i), measurement.getCurrentDurationAverage(i));
                    }
                    writeCountAndDuration(stats, operation, -1, measurement.getOverallCountAverage(), measurement.getOverallDurationAverage());
                end();
            }

            private void writeCountAndDuration(Stats stats, String operation, int intervalIndex, double count, double duration) throws IOException {
                boolean link = intervalIndex >= 0;
                String statsName = stats.getName();
                String href = page.url(null, statsName + "/operation", operation, statsName + "/interval", intervalIndex);

                start("td");
                    if (link) {
                        start("a", "href", href);
                    }
                    object(count).html("/s");
                    if (link) {
                        end();
                    }
                end();

                start("td");
                    if (link) {
                        start("a", "href", href);
                    }
                    if (Double.isNaN(duration)) {
                        start("span", "class", "label").html("N/A").end();
                    } else {
                        object(duration * 1e3).html("ms");
                    }
                    if (link) {
                        end();
                    }
                end();
            }
        };
    }
}
