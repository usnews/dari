package com.psddev.dari.db;

import com.psddev.dari.util.DebugFilter;
import com.psddev.dari.util.DependencyResolver;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.PullThroughCache;
import com.psddev.dari.util.StringLogger;
import com.psddev.dari.util.TypeDefinition;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@DebugFilter.Path("init")
@SuppressWarnings("serial")
public class InitializerServlet extends HttpServlet {

    private final Map<Class<?>, Initializer> INITIALIZERS = new PullThroughCache<Class<?>, Initializer>() {

        @Override
        protected Initializer produce(Class<?> initClass) {
            return (Initializer) TypeDefinition.getInstance(initClass).newInstance();
        }
    };

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        PrintWriter writer = response.getWriter();
        DependencyResolver<Initializer> initializersResolver = new DependencyResolver<Initializer>();

        for (Class<?> initClass : ObjectUtils.findClasses(Initializer.class)) {
            int mod = initClass.getModifiers();
            if (!(Modifier.isAbstract(mod) || Modifier.isInterface(mod))) {
                Initializer initializer = INITIALIZERS.get(initClass);
                Set<Class<? extends Initializer>> dependencies = initializer.dependencies();
                if (dependencies == null) {
                    initializersResolver.addRequired(initializer);
                } else {
                    for (Class<? extends Initializer> dependencyClass : dependencies) {
                        initializersResolver.addRequired(initializer, INITIALIZERS.get(dependencyClass));
                    }
                }
            }
        }

        Database database = Database.Static.getDefault();
        StringLogger logger = new StringLogger();
        try {

            for (Initializer initializer : initializersResolver.resolve()) {
                logger.reset();
                initializer.execute(database, logger);

                writer.print("<h2>Executing ");
                writer.print(initializer.getClass().getName());
                writer.print("</h2><p><pre>");
                writer.print(logger.toString());
                writer.println("</pre></p>");
            }

        } catch (Exception ex) {
            writer.println("<p><pre>");
            ex.printStackTrace(writer);
            writer.println("</pre></p>");
        }
    }
}
