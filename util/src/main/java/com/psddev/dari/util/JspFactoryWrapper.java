package com.psddev.dari.util;

import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.jsp.JspApplicationContext;
import javax.servlet.jsp.JspEngineInfo;
import javax.servlet.jsp.JspFactory;
import javax.servlet.jsp.PageContext;

/**
 * Skeletal JSP factory implementation that forwards all calls to a
 * delegate.
 */
public class JspFactoryWrapper extends JspFactory {

    private JspFactory delegate;

    /** Returns the delegate. */
    public JspFactory getDelegate() {
        return delegate;
    }

    /** Sets the delegate. */
    public void setDelegate(JspFactory delegate) {
        this.delegate = delegate;
    }

    // --- JspFactory support ---

    @Override
    public JspEngineInfo getEngineInfo() {
        return delegate.getEngineInfo();
    }

    @Override
    public JspApplicationContext getJspApplicationContext(ServletContext context) {
        return delegate.getJspApplicationContext(context);
    }

    @Override
    public PageContext getPageContext(Servlet servlet, ServletRequest request, ServletResponse response, String errorPageUrl, boolean needsSession, int buffer, boolean autoflush) {
        return delegate.getPageContext(servlet, request, response, errorPageUrl, needsSession, buffer, autoflush);
    }

    @Override
    public void releasePageContext(PageContext pageContext) {
        delegate.releasePageContext(pageContext);
    }
}
