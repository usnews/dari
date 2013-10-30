package com.psddev.dari.util;

import javax.servlet.jsp.JspException;
import javax.servlet.jsp.tagext.TagSupport;

public class AttributeTag extends TagSupport {

    private static final long serialVersionUID = 1L;

    private String name;
    private Object value;

    public void setName(String name) {
        this.name = name;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public int doStartTag() throws JspException {
        LocalTag localTag = (LocalTag) findAncestorWithClass(this, LocalTag.class);

        if (localTag != null) {
            localTag.setAttribute(name, value);

        } else {
            pageContext.getRequest().setAttribute(name, value);
        }

        return SKIP_BODY;
    }
}
