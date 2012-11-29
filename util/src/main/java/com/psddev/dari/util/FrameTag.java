package com.psddev.dari.util;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.jsp.JspException;
import javax.servlet.jsp.tagext.BodyTagSupport;

@SuppressWarnings("serial")
public class FrameTag extends BodyTagSupport {

    private static final String ATTRIBUTE_PREFIX = FrameTag.class.getName() + ".";
    private static final String JS_INCLUDED_PREFIX = ATTRIBUTE_PREFIX + "jsIncluded";

    private String name;
    private boolean lazy;

    public void setName(String name) {
        this.name = name;
    }

    public void setLazy(boolean lazy) {
        this.lazy = lazy;
    }

    // --- TagSupport support ---

    private boolean isRenderingFrame(HttpServletRequest request) {
        return request.getParameter(FrameFilter.PATH_PARAMETER) != null;
    }

    private void writeScript(HttpServletRequest request, HtmlWriter writer, String source) throws IOException {
        writer.start("script",
                "type", "text/javascript",
                "src", JspUtils.getAbsolutePath(request, source));
        writer.end();
    }

    private void startFrame(HttpServletRequest request, HtmlWriter writer, String... classNames) throws IOException {
        StringBuilder fullClassName = new StringBuilder("dari-frame");

        if (classNames != null) {
            int length = classNames.length;
            
            if (length > 0) {
                for (int i = 0; i < length; ++ i) {
                    fullClassName.append(" dari-frame-");
                    fullClassName.append(classNames[i]);
                }
            }
        }

        writer.start("div",
                "class", fullClassName.toString(),
                "name", name,
                "data-extra-form-data",
                        FrameFilter.PATH_PARAMETER + "=" + StringUtils.encodeUri(JspUtils.getCurrentServletPath(request)) + "&" +
                        FrameFilter.NAME_PARAMETER + "=" + StringUtils.encodeUri(name));
    }

    @Override
    public int doStartTag() throws JspException {
        HttpServletRequest request = (HttpServletRequest) pageContext.getRequest();

        try {
            if (!Boolean.TRUE.equals(request.getAttribute(JS_INCLUDED_PREFIX))) {
                request.setAttribute(JS_INCLUDED_PREFIX, Boolean.TRUE);

                @SuppressWarnings("all")
                HtmlWriter writer = new HtmlWriter(pageContext.getOut());

                writeScript(request, writer, "/_resource/jquery2/jquery.extra.js");
                writeScript(request, writer, "/_resource/jquery2/jquery.popup.js");
                writeScript(request, writer, "/_resource/jquery2/jquery.frame.js");
                writer.start("script", "type", "text/javascript");
                    writer.write("$(window.document).frame().ready(function(){$(this).trigger('create');});");
                writer.end();
            }

            if (!lazy ||
                    isRenderingFrame(request) ||
                    Boolean.FALSE.equals(ObjectUtils.to(Boolean.class, request.getParameter(FrameFilter.LAZY_PARAMETER)))) {
                return EVAL_BODY_BUFFERED;

            } else {
                @SuppressWarnings("all")
                HtmlWriter writer = new HtmlWriter(pageContext.getOut());

                startFrame(request, writer);
                    writer.start("a", "href", JspUtils.getAbsolutePath(request, "", FrameFilter.LAZY_PARAMETER, false));
                        writer.html("");
                    writer.end();
                writer.end();

                return SKIP_BODY;
            }

        } catch (IOException error) {
            throw new JspException(error);
        }
    }

    @Override
    public int doEndTag() throws JspException {
        if (bodyContent != null) {
            String body = bodyContent.getString();
            
            if (body != null) {
                HttpServletRequest request = (HttpServletRequest) pageContext.getRequest();

                if (isRenderingFrame(request)) {
                    request.setAttribute(FrameFilter.BODY_ATTRIBUTE, body);

                } else {
                    try {
                        @SuppressWarnings("all")
                        HtmlWriter writer = new HtmlWriter(pageContext.getOut());

                        startFrame(request, writer, "loaded");
                            writer.write(body);
                        writer.end();

                    } catch (IOException error) {
                        throw new JspException(error);
                    }
                }
            }
        }

        return EVAL_PAGE;
    }
}
