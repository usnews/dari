package com.psddev.dari.util;

public class CssDeclaration {

    private String property;
    private String value;

    public CssDeclaration(String property, String value) {
        this.property = property;
        this.value = value;
    }

    public String getProperty() {
        return property;
    }

    public String getValue() {
        return value;
    }

    // --- Object support ---

    @Override
    public String toString() {
        StringBuilder css = new StringBuilder();

        css.append(property);
        css.append(":");

        css.append(value);
        css.append(";");

        return css.toString();
    }
}
