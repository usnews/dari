package com.psddev.dari.util.sa;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.psddev.dari.util.ClassFinder;
import com.psddev.dari.util.TypeDefinition;

public abstract class JvmAnalyzer {

    private Jvm jvm;
    private JvmLogger logger;

    /**
     * @return Never {@code null}.
     */
    public Jvm getJvm() {
        return jvm;
    }

    /**
     * @return Never {@code null}.
     */
    public JvmLogger getLogger() {
        return logger;
    }

    /**
     * Called before the analysis starts.
     */
    public abstract void onStart();

    /**
     * Called after the analysis stops.
     */
    public abstract void onStop();

    /**
     * {@link JvmAnalyzer} utility methods.
     */
    public static class Static {

        /**
         * Analyzes all classes using all instances of {@link JvmAnalyzer}
         * classes and logs using the given {@code logger}.
         *
         * @param logger If {@code null}, doesn't log anything.
         */
        public static void analyzeAll(JvmLogger logger) throws IOException {
            Jvm jvm = new Jvm();
            Set<JvmAnalyzer> analyzers = new HashSet<JvmAnalyzer>();

            if (logger == null) {
                logger = new JvmLogger();
            }

            try {
                for (Class<? extends JvmAnalyzer> analyzerClass : ClassFinder.Static.findClasses(JvmAnalyzer.class)) {
                    JvmAnalyzer analyzer = TypeDefinition.getInstance(analyzerClass).newInstance();

                    analyzer.jvm = jvm;
                    analyzer.logger = logger;
                    analyzer.onStart();
                    analyzers.add(analyzer);
                }

                for (Class<?> c : ClassFinder.Static.findClasses(Object.class)) {
                    if (!c.getName().startsWith("com.psddev.")) {
                        jvm.analyze(c);
                    }
                }

            } finally {
                stopAnalyzers(analyzers);
            }
        }

        private static void stopAnalyzers(Set<JvmAnalyzer> analyzers) {
            Iterator<JvmAnalyzer> i = analyzers.iterator();

            if (i.hasNext()) {
                try {
                    i.next().onStop();

                } finally {
                    i.remove();
                    stopAnalyzers(analyzers);
                }
            }
        }
    }
}
