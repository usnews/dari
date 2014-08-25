package com.psddev.dari.maven;

import java.io.OutputStream;

class NullOutputStream extends OutputStream {

    @Override
    public void write(byte[] b) {
    }

    @Override
    public void write(byte[] b, int off, int len) {
    }

    @Override
    public void write(int b) {
    }
}
