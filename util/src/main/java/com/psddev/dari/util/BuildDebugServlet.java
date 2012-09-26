package com.psddev.dari.util;

import java.io.InputStream;
import java.io.IOException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/** Debug servlet for inspecting application build information. */
@DebugFilter.Path("build")
@SuppressWarnings("serial")
public class BuildDebugServlet extends HttpServlet {

    public static final String PROPERTIES_FILE = "/WEB-INF/classes/build.properties";

    /** Returns all the properties in the build file. */
    public static Properties getProperties(ServletContext context) throws IOException {
        return getEmbeddedProperties(context, null);
    }

    /** Returns all the properties in the build file of an embedded war file. */
    public static Properties getEmbeddedProperties(ServletContext context, String embeddedPath) throws IOException {
        Properties build = new Properties();
        InputStream stream = context.getResourceAsStream((embeddedPath != null ? embeddedPath : "") + PROPERTIES_FILE);
        if (stream != null) {
            try {
                build.load(stream);
            } finally {
                stream.close();
            }
        }
        return build;
    }

    /**
     * Returns a descriptive label that represents the build within the
     * given {@code context}.
     */
    public static String getLabel(ServletContext context) {
        Properties build = null;
        try {
            build = getProperties(context);
        } catch (IOException ex) {
        }
        if (build == null) {
            build = new Properties();
        }
        return getLabel(build);
    }

    // Returns a descriptive label using the given properties.
    private static String getLabel(Properties properties) {
        String title = properties.getProperty("name");
        if (ObjectUtils.isBlank(title)) {
            title = "Anonymous Application";
        }
        String version = properties.getProperty("version");
        if (!ObjectUtils.isBlank(version)) {
            title += ": " + version;
        }
        String buildNumber = properties.getProperty("buildNumber");
        if (!ObjectUtils.isBlank(buildNumber)) {
            title += " build " + buildNumber;
        }
        return title;
    }

    @Override
    protected void doGet(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        final String buildContext = request.getParameter("context");

        new DebugFilter.PageWriter(getServletContext(), request, response) {{
            startPage("Build Information");

                start("style", "type", "text/css");
                    write("tr.merge { color: rgba(0, 0, 0, 0.3); }");
                    write("td.num { text-align: right; }");
                    write("td:not(.wrap) { white-space: nowrap; }");
                end();

                start("script");
                    write("$(document).ready(function(){");
                        write("$('#contextPicker').change(function() {");
                            write("this.form.submit();");
                        write("});");
                    write("});");
                end();

                Map<String, Properties> embeddedProperties = new LinkedHashMap<String, Properties>();
                @SuppressWarnings("unchecked")
                Set<String> paths = (Set<String>) getServletContext().getResourcePaths("/");
                if (paths != null) {
                    for (String path : paths) {
                        if (path.endsWith("/")) {
                            path = path.substring(0, path.length()-1);
                            Properties properties = getEmbeddedProperties(getServletContext(), path);
                            if (!properties.isEmpty()) {
                                embeddedProperties.put(path.substring(1), properties);
                            }
                        }
                    }
                }

                Properties build = null;
                if (embeddedProperties.containsKey(buildContext)) {
                    build = embeddedProperties.get(buildContext);
                } else {
                    build = getProperties(getServletContext());
                }

                String issueSystem = build.getProperty("issueManagementSystem");
                String issueUrl = build.getProperty("issueManagementUrl");
                Pattern issuePattern = null;
                String issueUrlFormat = null;
                if ("JIRA".equals(issueSystem)) {
                    String prefix = "/browse/";
                    int prefixAt = issueUrl.indexOf(prefix);
                    if (prefixAt > -1) {
                        prefixAt += prefix.length();
                        int slashAt = issueUrl.indexOf("/", prefixAt);
                        String jiraId = slashAt > -1 ?
                                issueUrl.substring(prefixAt, slashAt) :
                                issueUrl.substring(prefixAt);
                        issuePattern = Pattern.compile("\\Q" + jiraId + "\\E-\\d+");
                        issueUrlFormat = issueUrl.substring(0, prefixAt) + "%s";
                    }
                }

                String scmUrlFormat = null;
                String scmConnection = build.getProperty("scmConnection");
                if (ObjectUtils.isBlank(scmConnection)) {
                    scmConnection = build.getProperty("scmDeveloperConnection");
                }
                if (!ObjectUtils.isBlank(scmConnection)) {
                    if (scmConnection.startsWith("scm:git:")) {
                        scmUrlFormat = build.getProperty("scmUrl") + "/commit/%s";
                    }
                }

                String commitsString = build.getProperty("gitCommits");
                Map<String, List<GitCommit>> commitsMap = new LinkedHashMap<String, List<GitCommit>>();
                if (!ObjectUtils.isBlank(commitsString)) {
                    String currRefNames = null;
                    for (String e : StringUtils.split(commitsString, "(?m)\\s*~-~\\s*")) {
                        GitCommit commit = new GitCommit(e, issuePattern);
                        String refNames = commit.refNames;
                        if (!ObjectUtils.isBlank(refNames)) {
                            if (refNames.startsWith("(")) {
                                refNames = refNames.substring(1);
                            }
                            if (refNames.endsWith(")")) {
                                refNames = refNames.substring(
                                        0, refNames.length() - 1);
                            }
                            currRefNames = refNames;
                        }
                        List<GitCommit> commits = commitsMap.get(currRefNames);
                        if (commits == null) {
                            commits = new ArrayList<GitCommit>();
                            commitsMap.put(currRefNames, commits);
                        }
                        commits.add(commit);
                    }
                }

                start("h2").html("Commits").end();

                start("form", "action", "", "method", "GET", "class", "form-inline");
                    html("For: ");
                    start("select", "style", "width:auto;", "id", "contextPicker", "name", "context", "class", "input-xlarge");
                        start("option", "value", "");
                            html(getLabel(getServletContext()));
                        end();
                        for (Map.Entry<String, Properties> entry : embeddedProperties.entrySet()) {
                            start("option", "value", entry.getKey(), "selected", entry.getKey().equals(buildContext) ? "selected" : null);
                                html(getLabel(entry.getValue()));
                            end();
                        }
                    end();
                end();

                if (commitsMap.isEmpty()) {
                    start("p", "class", "alert");
                        html("Not available!");
                    end();

                } else {
                    int colspan = 3;

                    start("table", "class", "table table-condensed table-striped");
                        start("thead");
                            start("tr");

                                start("th").html("Date").end();

                                if (issuePattern != null) {
                                    start("th").html("Issues").end();
                                    ++ colspan;
                                }

                                start("th").html("Author").end();
                                start("th").html("Subject").end();

                                if (scmUrlFormat != null) {
                                    start("th").html("SCM").end();
                                    ++ colspan;
                                }
                            end();
                        end();

                        start("tbody");
                            for (Map.Entry<String, List<GitCommit>> entry : commitsMap.entrySet()) {

                                start("tr");
                                    start("td", "class", "wrap", "colspan", colspan);
                                        start("strong").html(entry.getKey()).end();
                                    end();
                                end();

                                for (GitCommit commit : entry.getValue()) {
                                    start("tr", "class", commit.subject.startsWith("Merge branch ") ? "merge" : null);

                                        start("td").html(commit.date).end();

                                        if (issuePattern != null) {
                                            start("td");
                                                for (String issue : commit.issues) {
                                                    if (issueUrlFormat != null) {
                                                        start("a", "href", String.format(issueUrlFormat, issue), "target", "_blank");
                                                            html(issue);
                                                        end();
                                                    } else {
                                                        html(issue);
                                                    }
                                                    tag("br");
                                                }
                                            end();
                                        }

                                        start("td").html(commit.author).end();
                                        start("td", "class", "wrap").html(commit.subject).end();

                                        if (scmUrlFormat != null) {
                                            start("td");
                                                start("a", "href", String.format(scmUrlFormat, commit.hash), "target", "_blank");
                                                    html(commit.hash.substring(0, 6));
                                                end();
                                            end();
                                        }
                                    end();
                                }
                            }
                        end();
                    end();
                }

                start("h2").html("Resources").end();
                start("table", "class", "table table-condensed");
                    start("thead");
                        start("tr");
                            start("th").html("Path").end();
                            start("th").html("Size (Bytes)").end();
                            start("th").html("MD5").end();
                        end();
                    end();
                    start("tbody");
                        writeResourcesOfPath("", 0, "/");
                    end();
                end();

            endPage();
        }

            private void writeResourcesOfPath(String parentPath, int depth, String path) throws IOException {
                start("tr");
                start("td", "style", "padding-left: " + (depth * 20) + "px").html(path).end();

                if (path.endsWith("/")) {
                    start("td").end();
                    start("td").end();

                    @SuppressWarnings("unchecked")
                    List<String> subPaths = new ArrayList<String>((Set<String>) getServletContext().getResourcePaths(path));
                    Collections.sort(subPaths);

                    int subDepth = depth + 1;
                    for (String subPath : subPaths) {
                        writeResourcesOfPath(path, subDepth, subPath);
                    }

                    end();

                } else {
                    MessageDigest md5 = null;
                    try {
                        md5 = MessageDigest.getInstance("MD5");
                    } catch (NoSuchAlgorithmException ex) {
                    }

                    try {
                        InputStream input = getServletContext().getResourceAsStream(path);
                        if (input != null) {
                            try {

                                if (md5 != null) {
                                    input = new DigestInputStream(input, md5);
                                }

                                int totalBytesRead = 0;
                                int bytesRead = 0;
                                byte[] buffer = new byte[4096];
                                while ((bytesRead = input.read(buffer)) > 0) {
                                    totalBytesRead += bytesRead;
                                }

                                start("td", "class", "num").object(totalBytesRead).end();
                                start("td");
                                    if (md5 != null) {
                                        write(StringUtils.hex(md5.digest()));
                                    }
                                end();

                            } finally {
                                input.close();
                            }
                        }

                    } catch (IOException ex) {
                    }

                    end();
                }
            }
        };
    }

    private static class GitCommit {

        public String hash;
        public String author;
        public Date date;
        public String refNames;
        public String subject;
        public String body;
        public List<String> issues;

        public GitCommit(String line, Pattern issuePattern) {

            String[] items = StringUtils.split(line, "(?m)\\s*~\\|~\\s*");
            hash = items[0];
            author = items[1];
            Long timestamp = ObjectUtils.to(Long.class, items[2]);
            if (timestamp != null) {
                date = new Date(timestamp * 1000);
            }

            refNames = items.length > 3 ? items[3] : null;
            subject = items.length > 4 ? items[4] : null;
            body = items.length > 5 ? items[5] : null;

            if (issuePattern != null) {
                issues = new ArrayList<String>();
                for (String e : new String[] { subject, body }) {
                    if (e != null) {
                        Matcher matcher = issuePattern.matcher(e);
                        while (matcher.find()) {
                            issues.add(matcher.group(0));
                        }
                    }
                }
            }
        }
    }
}
