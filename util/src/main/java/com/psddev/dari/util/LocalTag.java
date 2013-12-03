package com.psddev.dari.util;

import java.util.Map;

import javax.servlet.ServletRequest;
import javax.servlet.jsp.JspException;
import javax.servlet.jsp.tagext.TagSupport;

public class LocalTag extends TagSupport {

    private static final long serialVersionUID = 1L;

    private transient final Map<String, Object> oldAttributes = new CompactMap<String, Object>();

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
        oldAttributes.clear();

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
}
