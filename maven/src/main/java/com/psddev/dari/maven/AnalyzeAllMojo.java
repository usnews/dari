package com.psddev.dari.maven;

import java.io.File;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.project.MavenProject;

import com.psddev.dari.util.ObjectUtils;

/**
 * @goal analyze-all
 * @requiresDependencyResolution compile
 */
public class AnalyzeAllMojo extends AbstractMojo {

    /**
     * @parameter expression="${project}"
     * @required
     * @readonly
     */
    protected MavenProject project;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        ClassLoader loader = ObjectUtils.getCurrentClassLoader();
        Method addUrlMethod = null;
        Log log = getLog();

        if (loader instanceof URLClassLoader) {
            try {
                addUrlMethod = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
                addUrlMethod.setAccessible(true);

            } catch (NoSuchMethodException error) {
                throw new MojoExecutionException(
                        "Can't find URLClassLoader#addURL method to change the current class loader!",
                        error);
            }
        }

        for (Object artifact : project.getArtifacts()) {
            try {
                addUrlMethod.invoke(loader, ((Artifact) artifact).getFile().toURI().toURL());

            } catch (Exception error) {
                throw new MojoExecutionException(
                        String.format("Can't include [%s] in the classpath!", artifact),
                        error);
            }
        }

        try {
            addUrlMethod.invoke(loader, new File(project.getBuild().getOutputDirectory()).toURI().toURL());

        } catch (Exception error) {
            throw new MojoExecutionException(
                    "Can't include project build output directory in the classpath!",
                    error);
        }

        PrintStream oldErr = System.err;

        System.setErr(new PrintStream(new NullOutputStream()));

        try {
            AnalyzeAllThread thread = new AnalyzeAllThread(log);

            thread.start();

            try {
                thread.join();

            } catch (InterruptedException error) {
                // Interrupted most likely by user so move on.
            }

            if (thread.getLogger().hasErrors()) {
                throw new MojoFailureException("");
            }

        } finally {
            System.setErr(oldErr);
        }
    }
}
