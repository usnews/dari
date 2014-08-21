package com.psddev.dari.util.sa;

import org.objectweb.asm.Type;

public class JvmObjectStatic extends JvmObject {

    public JvmObjectStatic(Type type) {
        super(type);
    }

    @Override
    protected Object doResolve() {
        return JvmRunner.typeToClass(type);
    }

    @Override
    public JvmObjectStatic clone() {
        return updateClone(new JvmObjectStatic(type));
    }

    @Override
    protected void appendTo(StringBuilder sb) {
        sb.append(typeToName(type));
    }
}
