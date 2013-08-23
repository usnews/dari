package com.psddev.dari.util;

/**
 * Item that can be processed using {@link PdfWriter}
 */
public interface PdfWriteable {
    public abstract String getPdfXsl();
    public abstract String getPdfXml();
}
