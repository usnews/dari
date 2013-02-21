package com.psddev.dari.util;

import java.io.IOException;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides debugging tools for troubleshooting the application.
 *
 * <p>This filter loads:</p>
 *
 * <ul>
 * <li>{@link SettingsOverrideFilter}</li>
 * <li>{@link SourceFilter}</li>
 * <li>{@link LogCaptureFilter}</li>
 * <li>{@link ResourceFilter}</li>
 * </ul>
 */
public class DebugFilter extends AbstractFilter {

    public static final String DEFAULT_INTERCEPT_PATH = "/_debug/";
    public static final String INTERCEPT_PATH_SETTING = "dari/debugFilterInterceptPath";

    public static final String DEBUG_PARAMETER = "_debug";
    public static final String PRODUCTION_PARAMETER = "_prod";

    private static final Logger LOGGER = LoggerFactory.getLogger(DebugFilter.class);
    private static final Pattern NO_FILE_PATTERN = Pattern.compile("File &quot;(.*)&quot; not found");

    public static final String WEB_INF_DEBUG = "/WEB-INF/_debug/";

    private final PullThroughValue<Map<String, ServletWrapper>>
            debugServletWrappers = new PullThroughValue<Map<String, ServletWrapper>>() {

        @Override
        protected Map<String, ServletWrapper> produce() {
            Map<String, ServletWrapper> wrappers = new TreeMap<String, ServletWrapper>();

            for (Class<?> servletClass : ObjectUtils.findClasses(Servlet.class)) {
                try {
                    if (Modifier.isAbstract(servletClass.getModifiers())) {
                        continue;
                    }

                    String path = null;
                    Path pathAnnotation = servletClass.getAnnotation(Path.class);
                    if (pathAnnotation != null) {
                        path = pathAnnotation.value();
                    }

                    if (ObjectUtils.isBlank(path)) {
                        Name nameAnnotation = servletClass.getAnnotation(Name.class);
                        if (nameAnnotation != null) {
                            path = nameAnnotation.value();
                        }
                    }

                    if (ObjectUtils.isBlank(path)) {
                        continue;
                    }

                    @SuppressWarnings("unchecked")
                    ServletWrapper wrapper = new ServletWrapper((Class<? extends Servlet>) servletClass);
                    wrappers.put(path, wrapper);

                } catch (Throwable ex) {
                    LOGGER.warn(String.format(
                            "Can't load debug servlet [%s]!",
                            servletClass.getName()), ex);
                }
            }

            LOGGER.info("Found debug servlets: {}", wrappers.keySet());
            return wrappers;
        }
    };

    // --- AbstractFilter support ---

    @Override
    protected Iterable<Class<? extends Filter>> dependencies() {
        List<Class<? extends Filter>> dependencies = new ArrayList<Class<? extends Filter>>();
        dependencies.add(SettingsOverrideFilter.class);
        dependencies.add(SourceFilter.class);
        dependencies.add(LogCaptureFilter.class);
        dependencies.add(ResourceFilter.class);
        return dependencies;
    }

    @Override
    protected void doDestroy() {
        if (debugServletWrappers.isProduced()) {
            for (ServletWrapper wrapper : debugServletWrappers.get().values()) {
                wrapper.destroyServlet();
            }
        }
    }

    @Override
    protected void doDispatch(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws Exception {

        if (Settings.isProduction()) {
            super.doDispatch(request, response, chain);

        } else {
            try {
                super.doDispatch(request, response, chain);

            } catch (final Throwable ex) {
                new PageWriter(getServletContext(), request, response) {{
                    String id = page.createId();
                    String servletPath = JspUtils.getCurrentServletPath(page.getRequest());

                    start("div", "id", id);
                        start("pre", "class", "alert alert-error");
                            String noFile = null;
                            if (ex instanceof ServletException) {
                                String message = ex.getMessage();
                                if (message != null) {
                                    Matcher noFileMatcher = NO_FILE_PATTERN.matcher(message);
                                    if (noFileMatcher.matches()) {
                                        noFile = noFileMatcher.group(1);
                                    }
                                }
                            }

                            if (noFile != null) {
                                start("strong").html(servletPath).end().html(" doesn't exist!");

                            } else {
                                html("Can't render ");
                                start("a",
                                        "target", "_blank",
                                        "href", DebugFilter.Static.getServletPath(page.getRequest(), "code",
                                                "action", "edit",
                                                "type", "JSP",
                                                "servletPath", servletPath));
                                    html(servletPath);
                                end();
                                html("!\n\n");

                                List<String> paramNames = page.paramNamesList();
                                if (!ObjectUtils.isBlank(paramNames)) {
                                    html("Parameters:\n");
                                    for (String name : paramNames) {
                                        for (String value : page.params(String.class, name)) {
                                            html(name);
                                            html('=');
                                            html(value);
                                            html('\n');
                                        }
                                    }
                                    html('\n');
                                }

                                object(ex);
                            }
                        end();
                    end();

                    start("script", "type", "text/javascript");
                        write("(function() {");
                            write("var f = document.createElement('iframe');");
                            write("f.frameBorder = '0';");
                            write("var fs = f.style;");
                            write("fs.background = 'transparent';");
                            write("fs.border = 'none';");
                            write("fs.overflow = 'hidden';");
                            write("fs.width = '100%';");
                            write("f.src = '");
                            write(page.js(JspUtils.getAbsolutePath(page.getRequest(), "/_resource/dari/alert.html", "id", id)));
                            write("';");
                            write("var a = document.getElementById('");
                            write(id);
                            write("');");
                            write("a.parentNode.insertBefore(f, a.nextSibling);");
                        write("})();");
                    end();
                }};
            }
        }
    }

    private static String getInterceptPath() {
        return StringUtils.ensureSurrounding(Settings.getOrDefault(String.class, INTERCEPT_PATH_SETTING, DEFAULT_INTERCEPT_PATH), "/");
    }

    private static boolean authenticate(HttpServletRequest request, HttpServletResponse response) {
        String realm = ObjectUtils.coalesce(
                Settings.get(String.class, "dari/debugRealm"),
                Settings.get(String.class, "servlet/debugRealm"),
                "Debugging Tool");

        String username = ObjectUtils.coalesce(
                Settings.get(String.class, "dari/debugUsername"),
                Settings.get(String.class, "servlet/debugUsername"));

        String password = ObjectUtils.coalesce(
                Settings.get(String.class, "dari/debugPassword"),
                Settings.get(String.class, "servlet/debugPassword"));

        return JspUtils.authenticateBasic(
                request,
                response,
                realm,
                username,
                password);
    }

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        String interceptPath = getInterceptPath();
        String path = request.getServletPath();
        if (path.equals(interceptPath.substring(0, interceptPath.length() - 1))) {
            JspUtils.redirect(request, response, interceptPath);
            return;

        } else if (!path.startsWith(interceptPath)) {
            chain.doFilter(request, response);
            return;
        }

        if (!authenticate(request, response)) {
            return;
        }

        final ServletContext context = getServletContext();
        final String pathInfo;
        String action = path.substring(interceptPath.length());
        int slashAt = action.indexOf('/');

        if (slashAt > -1) {
            pathInfo = action.substring(slashAt);
            action = action.substring(0, slashAt);

        } else {
            pathInfo = "/";
        }

        if (!ObjectUtils.isBlank(action)) {
            ServletWrapper wrapper = debugServletWrappers.get().get(action);
            if (wrapper != null) {
                wrapper.serviceServlet(new HttpServletRequestWrapper(request) {
                    @Override
                    public String getPathInfo() {
                        return pathInfo;
                    }
                }, response);
                return;
            }

            String actionJsp = WEB_INF_DEBUG + action;
            try {
                if (context.getResource(actionJsp) != null) {
                    JspUtils.forward(request, response, actionJsp);
                    return;
                }
            } catch (MalformedURLException ex) {
            }
        }

        new PageWriter(getServletContext(), request, response) {{
            startPage();
                start("div", "class", "row-fluid");

                    start("div", "class", "span3");
                        start("h2").html("Standard Tools").end();
                        start("ul");
                            for (Map.Entry<String, ServletWrapper> entry : debugServletWrappers.get().entrySet()) {
                                start("li");
                                    start("a", "href", entry.getKey());
                                        html(entry.getKey());
                                    end();
                                end();
                            }
                        end();
                    end();

                    start("div", "class", "span3");
                        start("h2").html("Custom Tools").end();

                        @SuppressWarnings("unchecked")
                        Set<String> resources = (Set<String>) context.getResourcePaths(WEB_INF_DEBUG);
                        if (resources != null && !resources.isEmpty()) {
                            Set<String> jsps = new TreeSet<String>();
                            for (String jsp : resources) {
                                if (jsp.endsWith(".jsp")) {
                                    jsps.add(jsp);
                                }
                            }

                            if (!jsps.isEmpty()) {
                                start("ul");
                                    for (String jsp : jsps) {
                                        jsp = jsp.substring(WEB_INF_DEBUG.length());
                                        start("li");
                                            start("a", "href", jsp);
                                                html(jsp);
                                            end();
                                        end();
                                    }
                                end();
                            }
                        }
                    end();

                    start("div", "class", "span6");
                        start("h2").html("Pings").end();

                        Map<String, Throwable> errors = new LinkedHashMap<String, Throwable>();
                        start("table", "class", "table table-condensed");
                            start("thead");
                                start("tr");
                                    start("th").html("Class").end();
                                    start("th").html("Status").end();
                                end();
                            end();
                            start("tbody");
                                for (Map.Entry<Class<?>, Throwable> entry : Ping.Static.pingAll().entrySet()) {
                                    String name = entry.getKey().getName();
                                    Throwable error = entry.getValue();

                                    start("tr");
                                        start("td").html(name).end();
                                        start("td");
                                            if (error == null) {
                                                start("span", "class", "label label-success").html("OK").end();
                                            } else {
                                                start("span", "class", "label label-important").html("ERROR").end();
                                                errors.put(name, error);
                                            }
                                        end();
                                    end();
                                }
                            end();
                        end();

                        if (!errors.isEmpty()) {
                            start("dl");
                                for (Map.Entry<String, Throwable> entry : errors.entrySet()) {
                                    start("dt").html("Error for ").html(entry.getKey()).end();
                                    start("dd").object(entry.getValue()).end();
                                }
                            end();
                        }
                    end();

                end();
            endPage();
        }};
    }

    /** {@link DebugFilter} utility methods. */
    public static final class Static {

        private Static() {
        }

        /**
         * Returns the path to the debugging servlet described in the
         * given parameters.
         */
        public static String getServletPath(HttpServletRequest request, String path, Object... parameters) {
            return JspUtils.getAbsolutePath(request, getInterceptPath() + path, parameters);
        }
    }

    /**
     * Specifies that the target servlet should be available through
     * the debugging interface at the given path {@code value}.
     */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface Path {
        String value();
    }

    /** Use {@link Path} instead. */
    @Deprecated
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface Name {
        String value();
    }

    /**
     * HTML writer that's specialized for writing debugging interface
     * pages.
     */
    public static class PageWriter extends HtmlWriter {

        protected final WebPageContext page;

        public PageWriter(WebPageContext page) throws IOException {
            super(JspUtils.getWriter(page.getResponse()));
            this.page = page;
            page.getResponse().setContentType("text/html");
            page.getResponse().setCharacterEncoding("UTF-8");
            putAllStandardDefaults();
        }

        public PageWriter(
                ServletContext context,
                HttpServletRequest request,
                HttpServletResponse response)
                throws IOException {

            this(new WebPageContext(context, request, response));
        }

        public void startHtml() throws IOException {
            tag("!DOCTYPE html");
            start("html");
        }

        public void startHead(String title) throws IOException {
            start("head");
                start("title").html(title).end();
        }

        public void includeStylesheet(String url) throws IOException {
            tag("link", "href", page.url(url), "rel", "stylesheet", "type", "text/css");
        }

        public void includeScript(String url) throws IOException {
            start("script", "src", page.url(url), "type", "text/javascript").end();
        }

        public void includeStandardStylesheetsAndScripts() throws IOException {
            includeStylesheet("/_resource/bootstrap/css/bootstrap.min.css");
            includeStylesheet("/_resource/codemirror/lib/codemirror.css");

            start("style", "type", "text/css");
                write("@font-face { font-family: 'AauxNextMedium'; src: url('/_resource/aauxnext-md-webfont.eot'); src: local('☺'), url('/_resource/aauxnext-md-webfont.woff') format('woff'), url('/_resource/aauxnext-md-webfont.ttf') format('truetype'), url('/_resource/aauxnext-md-webfont.svg#webfontfLsPAukW') }");
                write("body { word-wrap: break-word; }");
                write("select { word-wrap: normal; }");
                write("h1, h2, h3, h4, h5, h6 { font-family: AauxNextMedium, sans-serif; }");
                write(".navbar-inner { background: #0a5992; }");
                write(".navbar .brand { background: url(/_resource/bridge.png) no-repeat 55px 0; font-family: AauxNextMedium, sans-serif; height: 40px; line-height: 40px; margin: 0; min-width: 200px; padding: 0; }");
                write(".navbar .brand a { color: #fff; float: left; font-size: 30px; padding-right: 60px; text-transform: uppercase; }");
                write(".popup { width: 60%; }");
                write(".popup .content { background-color: white; -moz-border-radius: 5px; -webkit-border-radius: 5px; border-radius: 5px; -moz-box-shadow: 0 0 10px #777; -webkit-box-shadow: 0 0 10px #777; box-shadow: 0 0 10px #777; position: relative; top: 10px; }");
                write(".popup .content .marker { border-color: transparent transparent white transparent; border-style: solid; border-width: 10px; left: 5px; position: absolute; top: -20px; }");
                write(".CodeMirror-scroll { height: auto; overflow-x: auto; overflow-y: hidden; width: 100%; }");
                write(".CodeMirror pre { font-family: Menlo, Monaco, 'Courier New', monospace; font-size: 12px; }");
                write(".CodeMirror .selected { background-color: #FCF8E3; }");
                write(".CodeMirror .errorLine { background-color: #F2DEDE; }");
                write(".CodeMirror .errorColumn { background-color: #B94A48; color: white; }");
                write(".json { position: relative; }");
                write(".json:after { background: #ccc; content: 'JSON'; font-size: 9px; line-height: 9px; padding: 4px; position: absolute; right: 0; top: 0; }");
            end();

            includeScript("/_resource/jquery/jquery-1.7.1.min.js");
            includeScript("/_resource/jquery/jquery.livequery.js");
            includeScript("/_resource/jquery/jquery.misc.js");
            includeScript("/_resource/jquery/jquery.frame.js");
            includeScript("/_resource/jquery/jquery.popup.js");
            includeScript("/_resource/codemirror/lib/codemirror.js");
            includeScript("/_resource/codemirror/mode/clike.js");

            start("script", "type", "text/javascript");
                write("$(function() {");
                    write("$('body').frame();");
                write("});");
            end();
        }

        public void endHead() throws IOException {
            end();
            flush();
        }

        public void startBody(String... titles) throws IOException {
            start("body");
                start("div", "class", "navbar navbar-fixed-top");
                    start("div", "class", "navbar-inner");
                        start("div", "class", "container-fluid");
                            start("span", "class", "brand");
                                start("a", "href", DebugFilter.Static.getServletPath(page.getRequest(), ""));
                                    html("Dari");
                                end();
                                if (!ObjectUtils.isBlank(titles)) {
                                    for (int i = 0, length = titles.length; i < length; ++ i) {
                                        String title = titles[i];
                                        html(title);
                                        if (i + 1 < length) {
                                            html(" \u2192 ");
                                        }
                                    }
                                }
                            end();
                        end();
                    end();
                end();
                start("div", "class", "container-fluid", "style", "padding-top: 54px;");
        }

        public void endBody() throws IOException {
                end();
            end();
        }

        public void endHtml() throws IOException {
            end();
        }

        /** Writes all necessary elements to start the page. */
        public void startPage(String... titles) throws IOException {
            startHtml();
                startHead(ObjectUtils.isBlank(titles) ? null : titles[0]);
                    includeStandardStylesheetsAndScripts();
                endHead();
                startBody(titles);
        }

        /** Writes all necessary elements to end the page. */
        public void endPage() throws IOException {
                endBody();
            endHtml();
        }
    }

    private class ServletWrapper implements ServletConfig {

        private final Servlet servlet;
        private final AtomicBoolean isInitialized = new AtomicBoolean();

        public ServletWrapper(Class<? extends Servlet> servletClass) {
            this.servlet = TypeDefinition.getInstance(servletClass).newInstance();
        }

        public void serviceServlet(
                HttpServletRequest request,
                HttpServletResponse response)
                throws IOException, ServletException {

            if (isInitialized.compareAndSet(false, true)) {
                servlet.init(this);
                LOGGER.info("Initialized [{}] servlet", getServletName());
            }
            servlet.service(request, response);
        }

        public void destroyServlet() {
            if (isInitialized.compareAndSet(true, false)) {
                servlet.destroy();
                LOGGER.info("Destroyed [{}] servlet", getServletName());
            }
        }

        // --- ServletConfig support ---

        @Override
        public String getInitParameter(String name) {
            return null;
        }

        @Override
        public Enumeration<String> getInitParameterNames() {
            return Collections.enumeration(Collections.<String>emptyList());
        }

        @Override
        public ServletContext getServletContext() {
            return DebugFilter.this.getServletContext();
        }

        @Override
        public String getServletName() {
            return getFilterConfig().getFilterName() + "$" + servlet.getClass().getName();
        }
    }

    /** Overrides certain settings based on request parameters. */
    private static class SettingsOverrideFilter extends AbstractFilter {

        @Override
        protected void doRequest(
                HttpServletRequest request,
                HttpServletResponse response,
                FilterChain chain)
                throws IOException, ServletException {

            try {
                Boolean production = ObjectUtils.to(Boolean.class, request.getParameter(PRODUCTION_PARAMETER));
                if (production != null && !authenticate(request, response)) {
                    return;
                }

                Settings.setOverride(Settings.PRODUCTION_SETTING, production);

                if (ObjectUtils.to(boolean.class, request.getParameter(DEBUG_PARAMETER))) {
                    if (authenticate(request, response)) {
                        Settings.setOverride(Settings.DEBUG_SETTING, Boolean.TRUE);
                        if (production == null) {
                            Settings.setOverride(Settings.PRODUCTION_SETTING, Boolean.FALSE);
                        }
                    } else {
                        return;
                    }
                }

                chain.doFilter(request, response);

            } finally {
                Settings.setOverride(Settings.PRODUCTION_SETTING, null);
                Settings.setOverride(Settings.DEBUG_SETTING, null);
            }
        }
    }
}
