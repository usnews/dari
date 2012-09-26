package ${package};

import com.psddev.dari.db.*;
import com.psddev.dari.util.*;
import java.util.*;
import javax.servlet.*;
import javax.servlet.http.*;

public class DefaultRouter extends AbstractFilter {

    @Override
    protected Iterable<Class<? extends Filter>> dependencies() {
        List<Class<? extends Filter>> dependencies = new ArrayList<Class<? extends Filter>>();
        dependencies.add(ApplicationFilter.class);
        return dependencies;
    }

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws Exception {

        // Renders welcome page in a non-production environment.
        // This should be replaced with your own application logic.
        if (Settings.isProduction() || !"/".equals(request.getServletPath())) {
            chain.doFilter(request, response);

        } else {
            new DebugFilter.PageWriter(getServletContext(), request, response).welcome();
        }
    }
}
