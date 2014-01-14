package com.psddev.dari.util;

import javax.servlet.jsp.JspException;
import javax.servlet.jsp.tagext.TagSupport;
import javax.servlet.jsp.tagext.TryCatchFinally;

public class AttributeTag extends TagSupport implements TryCatchFinally {

    private static final long serialVersionUID = 1L;

    private String name;
    private Object value;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public int doStartTag() throws JspException {
        LocalTag localTag = (LocalTag) findAncestorWithClass(this, LocalTag.class);
        String name = getName();
        Object value = getValue();

        if (localTag != null) {
            localTag.setAttribute(name, value);

        } else {
            pageContext.getRequest().setAttribute(name, value);
        }

        return SKIP_BODY;
    }

    @Override
    public void doCatch(Throwable error) throws Throwable {
        throw error;
    }

    @Override
    public void doFinally() {
        setName(null);
        setValue(null);
    }
}
