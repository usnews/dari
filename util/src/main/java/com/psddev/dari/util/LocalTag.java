package com.psddev.dari.util;

import java.util.Map;

import javax.servlet.ServletRequest;
import javax.servlet.jsp.JspException;
import javax.servlet.jsp.tagext.TagSupport;
import javax.servlet.jsp.tagext.TryCatchFinally;

public class LocalTag extends TagSupport implements TryCatchFinally {

    private static final long serialVersionUID = 1L;

    private final transient Map<String, Object> oldAttributes = new CompactMap<String, Object>();

    public void setAttribute(String name, Object value) {
        if (!ObjectUtils.isBlank(name)) {
            ServletRequest request = pageContext.getRequest();

            if (!oldAttributes.containsKey(name)) {
                oldAttributes.put(name, request.getAttribute(name));
            }

            request.setAttribute(name, value);
        }
    }

    @Override
    public int doStartTag() throws JspException {
        doFinally();
        return EVAL_BODY_INCLUDE;
    }

    @Override
    public int doEndTag() throws JspException {
        ServletRequest request = pageContext.getRequest();

        for (Map.Entry<String, Object> entry : oldAttributes.entrySet()) {
            request.setAttribute(entry.getKey(), entry.getValue());
        }

        return EVAL_PAGE;
    }

    @Override
    public void doCatch(Throwable error) throws Throwable {
        throw error;
    }

    @Override
    public void doFinally() {
        oldAttributes.clear();
    }
}
