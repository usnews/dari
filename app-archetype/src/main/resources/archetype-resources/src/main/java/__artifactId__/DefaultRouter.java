package ${package}.${artifactId};

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

        chain.doFilter(request, response);
    }
}
