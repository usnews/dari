package com.psddev.dari.util.sa;

public abstract class JvmObjectListener {

    public abstract void onBinary(int opcode, JvmObject other, boolean reverse);
}
