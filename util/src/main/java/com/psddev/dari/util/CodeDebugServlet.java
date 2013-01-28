package com.psddev.dari.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;

@DebugFilter.Path("code")
public class CodeDebugServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    public static final String INPUTS_ATTRIBUTE = CodeDebugServlet.class.getName() + ".inputs";

    private static final String WEB_INF_CLASSES_PATH = "/WEB-INF/classes/";
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<Map<String, Object>>() { };

    @Override
    protected void service(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        WebPageContext page = new WebPageContext(this, request, response);
        String action = page.param(String.class, "action");

        try {
        if ("run".equals(action)) {
            if (page.param(String.class, "isSave") != null) {
                doSave(page);

            } else {
                page.paramOrDefault(Type.class, "type", Type.JAVA).run(page);
            }

        } else {
            doEdit(page);
        }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static File getFile(WebPageContext page) {
        String file = page.param(String.class, "file");

        if (file == null) {
            String servletPath = page.param(String.class, "servletPath");

            if (servletPath != null) {
                file = page.getServletContext().getRealPath(servletPath);
            }
        }

        if (file == null) {
            return null;
        }

        File fileInstance = new File(file);

        if (!fileInstance.exists()) {
            File parent = fileInstance.getParentFile();
            parent.mkdirs();
        }

        return fileInstance;
    }

    private void doSave(WebPageContext page) throws IOException, ServletException {
        if (!page.isFormPost()) {
            throw new IllegalArgumentException("Must post!");
        }

        new DebugFilter.PageWriter(page) {{
            File file = getFile(page);
            ErrorUtils.errorIfNull(file, "file");
            String code = page.paramOrDefault(String.class, "code", "");

            try {
                CLASS_FOUND:
                    if (file.isDirectory()) {
                        Object result = CodeUtils.evaluateJava(code);

                        if (result instanceof Collection) {
                            for (Object item : (Collection<?>) result) {
                                if (item instanceof Class) {
                                    file = new File(file, ((Class<?>) item).getName().replace('.', File.separatorChar) + ".java");
                                    file.getParentFile().mkdirs();
                                    break CLASS_FOUND;
                                }
                            }
                        }

                        throw new IllegalArgumentException("Syntax error!");
                    }

                if (!file.exists()) {
                    file.createNewFile();
                }

                FileOutputStream fileOutput = new FileOutputStream(file);

                try {
                    fileOutput.write(code.replaceAll("(?:\r\n|[\r\n])", "\n").getBytes("UTF-8"));
                    start("p", "class", "alert alert-success");
                        html("Saved Successfully! (");
                        object(new Date());
                        html(")");
                    end();

                } finally {
                    fileOutput.close();
                }

            } catch (Exception ex) {
                start("pre", "class", "alert alert-error");
                    object(ex);
                end();
            }
        }};
    }

    private void doEdit(WebPageContext page) throws IOException, ServletException {
        final Type type = page.paramOrDefault(Type.class, "type", Type.JAVA);
        final File file = getFile(page);
        final StringBuilder codeBuilder = new StringBuilder();

        if (file != null) {
            if (file.exists()) {
                if (file.isDirectory()) {

                } else {
                    codeBuilder.append(IoUtils.toString(file, StringUtils.UTF_8));
                }

            } else {
                String filePath = file.getPath();

                if (filePath.endsWith(".java")) {
                    filePath = filePath.substring(0, filePath.length() - 5);

                    for (File sourceDirectory : CodeUtils.getSourceDirectories()) { String sourceDirectoryPath = sourceDirectory.getPath();

                        if (filePath.startsWith(sourceDirectoryPath)) {
                            String classPath = filePath.substring(sourceDirectoryPath.length());

                            if (classPath.startsWith(File.separator)) {
                                classPath = classPath.substring(1);
                            }

                            int lastSepAt = classPath.lastIndexOf(File.separatorChar);

                            if (lastSepAt < 0) {
                                codeBuilder.append("public class ");
                                codeBuilder.append(classPath);

                            } else {
                                codeBuilder.append("package ");
                                codeBuilder.append(classPath.substring(0, lastSepAt).replace(File.separatorChar, '.'));
                                codeBuilder.append(";\n\npublic class ");
                                codeBuilder.append(classPath.substring(lastSepAt + 1));
                            }

                            codeBuilder.append(" {\n}");
                            break;
                        }
                    }
                }
            }

        } else {
            Set<String> packages = findPackages();
            packages.add("com.psddev.dari.db.");
            packages.add("com.psddev.dari.util.");
            packages.add("java.util.");

            for (String packageName : packages) {
                codeBuilder.append("import ");
                codeBuilder.append(packageName);
                codeBuilder.append("*;\n");
            }

            codeBuilder.append("\n");
            codeBuilder.append("public class Code {\n");
            codeBuilder.append("    public static Object main() throws Throwable {\n");

            String query = page.param(String.class, "query");
            String objectClass = page.paramOrDefault(String.class, "objectClass", "Object");

            if (query == null) {
                codeBuilder.append("        return null;\n");

            } else {
                codeBuilder.append("        Query<").append(objectClass).append("> query = ").append(query).append(";\n");
                codeBuilder.append("        PaginatedResult<").append(objectClass).append("> result = query.select(0L, 10);\n");
                codeBuilder.append("        return result;\n");
            }

            codeBuilder.append("    }\n");
            codeBuilder.append("}\n");
        }

        new DebugFilter.PageWriter(page) {{
            List<Object> inputs = CodeDebugServlet.Static.getInputs(getServletContext());
            Object input = inputs == null || inputs.isEmpty() ? null : inputs.get(0);
            String name;
            
            if (file == null) {
                name = null;

            } else {
                name = file.toString();
                int slashAt = name.lastIndexOf('/');

                if (slashAt > -1) {
                    name = name.substring(slashAt + 1);
                }
            }

            startPage("Code Editor", name);
                start("div", "class", "row-fluid");

                    if (input != null) {
                        start("div", "class", "codeInput", "style", "bottom: 65px; position: fixed; top: 55px; width: 18%; z-index: 1000;");
                            start("h2").html("Input").end();
                            start("div", "style", "bottom: 0; overflow: auto; position: absolute; top: 38px; width: 100%;");
                                object(input);
                            end();
                        end();
                        start("style", "type", "text/css");
                            write(".codeInput pre { white-space: pre; word-break: normal; word-wrap: normal; }");
                        end();
                        start("script", "type", "text/javascript");
                            write("$('.codeInput').hover(function() {");
                                write("$(this).css('width', '50%');");
                            write("}, function() {");
                                write("$(this).css('width', '18%');");
                            write("});");
                        end();
                    }

                    start("div",
                            "class", input != null ? "span9" : "span12",
                            "style", input != null ? "margin-left: 20%" : null);
                        start("form",
                                "action", page.url(null),
                                "class", "code",
                                "method", "post",
                                "style", "margin-bottom: 70px;",
                                "target", "result");
                            tag("input", "name", "action", "type", "hidden", "value", "run");
                            tag("input", "name", "type", "type", "hidden", "value", type);
                            tag("input", "name", "file", "type", "hidden", "value", file);
                            tag("input", "name", "jspPreviewUrl", "type", "hidden", "value", page.param(String.class, "jspPreviewUrl"));

                            start("textarea", "name", "code");
                                html(codeBuilder);
                            end();
                            start("div",
                                    "class", "form-actions",
                                    "style", "bottom: 0; left: 0; margin: 0; padding: 10px 20px; position:fixed; right: 0; z-index: 1000;");
                                tag("input", "class", "btn btn-primary", "type", "submit", "value", "Run");
                                start("label", "class", "checkbox", "style", "display: inline-block; margin-left: 10px; white-space: nowrap;");
                                    tag("input", "name", "isLiveResult", "type", "checkbox");
                                    html("Live Result");
                                end();
                                tag("input",
                                        "class", "btn btn-success pull-right",
                                        "name", "isSave",
                                        "type", "submit",
                                        "value", "Save");
                            end();
                        end();

                        start("div",
                                "class", "resultContainer",
                                "style",
                                        "background: rgba(255, 255, 255, 0.8);" +
                                        "border-color: rgba(0, 0, 0, 0.2);" +
                                        "border-style: solid;" +
                                        "border-width: 0 0 0 1px;" +
                                        "max-height: 45%;" +
                                        "top: 55px;" +
                                        "overflow: auto;" +
                                        "padding: 0px 20px 5px 10px;" +
                                        "position: fixed;" +
                                        "right: 0px;" +
                                        "width: 35%;");
                            start("h2").html("Result").end();
                            start("div", "class", "frame", "name", "result");
                            end();
                        end();

                        start("script", "type", "text/javascript");
                            write("$('body').frame();");
                            write("var $codeForm = $('form.code');");
                            write("setTimeout(function() { $codeForm.submit(); }, 0);");

                            write("var lineMarkers = [ ];");
                            write("var columnMarkers = [ ];");
                            write("var codeMirror = CodeMirror.fromTextArea($('textarea')[0], {");
                                write("'indentUnit': 4,");
                                write("'lineNumbers': true,");
                                write("'lineWrapping': true,");
                                write("'matchBrackets': true,");
                                write("'mode': 'text/x-java',");
                                write("'onChange': $.throttle(1000, function() {");
                                    write("if ($codeForm.find(':checkbox[name=isLiveResult]').is(':checked')) {");
                                        write("$codeForm.submit();");
                                    write("}");
                                write("})");
                            write("});");

                            int line = page.param(int.class, "line");
                            if (line > 0) {
                                write("var line = ");
                                write(String.valueOf(line));
                                write(" - 1;");
                                write("codeMirror.setCursor(line);");
                                write("codeMirror.setLineClass(line, 'selected', 'selected');");
                                write("$(window).scrollTop(codeMirror.cursorCoords().y - $(window).height() / 2);");
                            }

                            write("var $resultContainer = $('.resultContainer');");
                            write("$resultContainer.find('.frame').bind('load', function() {");
                                write("$.each(lineMarkers, function() { codeMirror.clearMarker(this); codeMirror.setLineClass(this, null, null); });");
                                write("$.each(columnMarkers, function() { this.clear(); });");
                                write("var $frame = $(this).find('.syntaxErrors li').each(function() {");
                                    write("var $error = $(this);");
                                    write("var line = parseInt($error.attr('data-line')) - 1;");
                                    write("var column = parseInt($error.attr('data-column')) - 1;");
                                    write("if (line > -1 && column > -1) {");
                                        write("lineMarkers.push(codeMirror.setMarker(line, '!'));");
                                        write("codeMirror.setLineClass(line, 'errorLine', 'errorLine');");
                                        write("columnMarkers.push(codeMirror.markText({ 'line': line, 'ch': column }, { 'line': line, 'ch': column + 1 }, 'errorColumn'));");
                                    write("}");
                                write("});");
                            write("});");
                        end();

                    end();
                end();
            endPage();
        }

            @Override
            public void startBody(String... titles) throws IOException {
                start("body");
                    start("div", "class", "navbar navbar-fixed-top");
                        start("div", "class", "navbar-inner");
                            start("div", "class", "container-fluid");
                                start("span", "class", "brand");
                                    start("a", "href", DebugFilter.Static.getServletPath(page.getRequest(), ""));
                                        html("Dari");
                                    end();
                                    html("Code Editor \u2192 ");
                                end();

                                start("form",
                                        "action", page.url(null),
                                        "method", "get",
                                        "style", "float: left; height: 40px; line-height: 40px; margin: 0; padding-left: 10px;");
                                    start("select",
                                            "class", "span6",
                                            "name", "file",
                                            "onchange", "$(this).closest('form').submit();");
                                        start("option", "value", "");
                                            html("PLAYGROUND");
                                        end();
                                        for (File sourceDirectory : CodeUtils.getSourceDirectories()) {
                                            start("optgroup", "label", sourceDirectory);
                                                start("option",
                                                        "selected", sourceDirectory.equals(file) ? "selected" : null,
                                                        "value", sourceDirectory);
                                                    html("NEW CLASS IN ").html(sourceDirectory);
                                                end();
                                                writeFileOption(file, sourceDirectory, sourceDirectory);
                                            end();
                                        }
                                    end();
                                end();

                                includeStylesheet("/_resource/chosen/chosen.css");
                                includeScript("/_resource/chosen/chosen.jquery.min.js");
                                start("script", "type", "text/javascript");
                                    write("(function() {");
                                        write("$('select[name=file]').chosen({ 'search_contains': true });");
                                    write("})();");
                                end();
                            end();
                        end();
                    end();
                    start("div", "class", "container-fluid", "style", "padding-top: 54px;");
            }

            private void writeFileOption(File file, File sourceDirectory, File source) throws IOException {
                if (source.isDirectory()) {
                    for (File child : source.listFiles()) {
                        writeFileOption(file, sourceDirectory, child);
                    }

                } else {
                    start("option",
                            "selected", source.equals(file) ? "selected" : null,
                            "value", source);
                        html(source.toString().substring(sourceDirectory.toString().length()));
                    end();
                }
            }
        };
    }

    private Set<String> findPackages() {
        Set<String> packages = new TreeSet<String>();
        addPackages(packages, WEB_INF_CLASSES_PATH.length(), WEB_INF_CLASSES_PATH);
        return packages;
    }

    @SuppressWarnings("unchecked")
    private void addPackages(Set<String> packages, int prefixLength, String path) {
        for (String subPath : (Set<String>) getServletContext().getResourcePaths(path)) {
            if (subPath.endsWith("/")) {
                addPackages(packages, prefixLength, subPath);
            } else if (subPath.endsWith(".class")) {
                packages.add(path.substring(prefixLength).replace('/', '.'));
            }
        }
    }

    private enum Type {

        JAVA("Java") {
            @Override
            public void run(WebPageContext page) throws IOException, ServletException {

                new DebugFilter.PageWriter(page) {{
                    try {
                        Object result = CodeUtils.evaluateJava(page.paramOrDefault(String.class, "code", ""));

                        if (result instanceof DiagnosticCollector) {
                            start("pre", "class", "alert alert-error");
                                html("Syntax error!\n\n");
                                start("ol", "class", "syntaxErrors");
                                    for (Diagnostic<?> diagnostic : ((DiagnosticCollector<?>) result).getDiagnostics()) {
                                        if (diagnostic.getKind() == Diagnostic.Kind.ERROR) {
                                            start("li", "data-line", diagnostic.getLineNumber(), "data-column", diagnostic.getColumnNumber());
                                                html(diagnostic.getMessage(null));
                                            end();
                                        }
                                    }
                                end();
                            end();

                        } else if (result instanceof Collection) {
                            for (Object item : (Collection<?>) result) {

                                if (item instanceof Class) {
                                    List<Object> inputs = CodeDebugServlet.Static.getInputs(page.getServletContext());
                                    Object input = inputs == null || inputs.isEmpty() ? null : inputs.get(0);

                                    if (input != null) {
                                        Class<?> inputClass = input.getClass();
                                        Class<?> itemClass = (Class<?>) item;

                                        for (Method method : ((Class<?>) item).getDeclaredMethods()) {
                                            Class<?>[] parameterClasses = method.getParameterTypes();

                                            if (parameterClasses.length == 1 &&
                                                    parameterClasses[0].isAssignableFrom(inputClass) &&
                                                    method.getReturnType().isAssignableFrom(inputClass)) {
                                                Map<String, Object> inputMap = ObjectUtils.to(MAP_TYPE, input);
                                                Map<String, Object> processedMap = ObjectUtils.to(MAP_TYPE, method.invoke(itemClass.newInstance(), input));

                                                Set<String> keys = new HashSet<String>(inputMap.keySet());
                                                keys.addAll(processedMap.keySet());
                                                for (String key : keys) {
                                                    Object inputValue = inputMap.get(key);
                                                    Object processedValue = processedMap.get(key);
                                                    if (ObjectUtils.equals(inputValue, processedValue)) {
                                                        processedMap.remove(key);
                                                    }
                                                }

                                                result = processedMap;
                                                break;
                                            }
                                        }
                                    }
                                }
                            }

                            object(result);

                        } else {
                            object(result);
                        }

                    } catch (Exception ex) {
                        start("pre", "class", "alert alert-error");
                            object(ex);
                        end();
                    }
                }};
            }
        },

        JSP("JSP") {

            private final Map<String, Integer> draftIndexes = new HashMap<String, Integer>();

            @Override
            public void run(WebPageContext page) throws IOException, ServletException {

                new DebugFilter.PageWriter(page) {{
                    ServletContext context = page.getServletContext();
                    File file = getFile(page);
                    if (file == null) {
                        throw new IllegalArgumentException();
                    }

                    String servletPath = file.toString().substring(context.getRealPath("/").length() - 1).replace(File.separatorChar, '/');
                    String draft = "/WEB-INF/_draft" + servletPath;

                    int dotAt = draft.indexOf('.');
                    String extension;
                    if (dotAt < 0) {
                        extension = "";
                    } else {
                        extension = draft.substring(dotAt);
                        draft = draft.substring(0, dotAt);
                    }

                    synchronized (draftIndexes) {
                        Integer draftIndex = draftIndexes.get(draft);
                        if (draftIndex == null) {
                            draftIndex = 0;
                            draftIndexes.put(draft, draftIndex);
                        }
                        new File(context.getRealPath(draft + draftIndex + extension)).delete();
                        ++ draftIndex;
                        draftIndexes.put(draft, draftIndex);
                        draft = draft + draftIndex + extension;
                    }

                    String realDraft = context.getRealPath(draft);
                    File realDraftFile = new File(realDraft);
                    realDraftFile.getParentFile().mkdirs();
                    FileOutputStream realDraftOutput = new FileOutputStream(realDraftFile);
                    try {
                        realDraftOutput.write(page.paramOrDefault(String.class, "code", "").getBytes("UTF-8"));
                    } finally {
                        realDraftOutput.close();
                    }

                    page.getResponse().sendRedirect(
                            StringUtils.addQueryParameters(
                                    page.param(String.class, "jspPreviewUrl"),
                                    "_jsp", servletPath,
                                    "_draft", draft));
                }};
            }
        };

        private final String displayName;

        private Type(String displayName) {
            this.displayName = displayName;
        }

        public abstract void run(WebPageContext page) throws IOException, ServletException;

        // --- Object support ---

        @Override
        public String toString() {
            return displayName;
        }
    }

    /** {@link CodeDebugServlet} utility methods. */
    public static final class Static {

        private Static() {
        }

        @SuppressWarnings("unchecked")
        public static List<Object> getInputs(ServletContext context) {
            return (List<Object>) context.getAttribute(INPUTS_ATTRIBUTE);
        }

        public static void setInputs(ServletContext context, List<Object> inputs) {
            context.setAttribute(INPUTS_ATTRIBUTE, inputs);
        }
    }
}
