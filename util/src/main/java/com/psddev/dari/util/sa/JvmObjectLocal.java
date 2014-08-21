package com.psddev.dari.util.sa;

import org.objectweb.asm.Type;

public class JvmObjectLocal extends JvmObject {

    private final String name;

    public JvmObjectLocal(Type type, String name) {
        super(type);

        this.name = name;
    }

    @Override
    protected Object doResolve() {
        if (name.equals("this")) {
            Class<?> objectClass = JvmRunner.typeToClass(type);

            if (objectClass != null) {
                try {
                    return objectClass.newInstance();

                } catch (InstantiationException error) {
                } catch (IllegalAccessException error) {
                }
            }
        }

        return null;
    }

    @Override
    public JvmObjectLocal clone() {
        return updateClone(new JvmObjectLocal(type, name));
    }

    @Override
    protected void appendTo(StringBuilder sb) {
        sb.append(name);
    }
}
