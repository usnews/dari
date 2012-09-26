package com.psddev.dari.util;

import java.io.IOException;

import java.util.Map;
import java.util.TreeMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/** Debug servlet for inspecting {@linkplain Settings global settings}. */
@DebugFilter.Path("settings")
public class SettingsDebugServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    // --- HttpServlet support ---

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        new DebugFilter.PageWriter(getServletContext(), request, response) {{
            startPage("Settings");

            start("table", "class", "table table-condensed table-striped");
                start("thead");
                    start("tr");
                        start("th").html("Key").end();
                        start("th").html("Value").end();
                        start("th").html("Class").end();
                    end();
                end();

                start("tbody");
                    for (Map.Entry<String, Object> entry : flatten(Settings.asMap()).entrySet()) {
                        String key = entry.getKey();
                        String keyLowered = key.toLowerCase();
                        Object value = entry.getValue();

                        start("tr");
                            start("td").html(key).end();

                            start("td");
                            if (keyLowered.contains("password") ||
                                    keyLowered.contains("secret")) {
                                start("span", "class", "label label-warning").html("Hidden").end();
                            } else {
                                html(value);
                            }

                            start("td");
                                html(value != null ? value.getClass().getName() : "N/A");
                            end();
                        end();
                    }
                end();
            end();

            endPage();
        }};
    }

    private Map<String, Object> flatten(Map<String, Object> map) {
        Map<String, Object> flattened = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
        collectFlattenedValues(flattened, null, map);
        return flattened;
    }

    private void collectFlattenedValues(Map<String, Object> flattened, String key, Object value) {
        if (value instanceof Map) {
            String prefix = key == null ? "" : key + "/";
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                collectFlattenedValues(flattened, prefix + entry.getKey(), entry.getValue());
            }
        } else {
            flattened.put(key, value);
        }
    }
}
