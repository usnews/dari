package com.psddev.dari.util.sa;

public class JvmObjectArrayLength extends JvmObject {

    private final JvmObject object;

    public JvmObjectArrayLength(JvmObject object) {
        super(object.type);

        this.object = object;
    }

    @Override
    protected Object doResolve() {
        Object object = this.object.resolve();

        if (object instanceof Object[]) {
            return ((Object[]) object).length;

        } else {
            return null;
        }
    }

    @Override
    public JvmObjectArrayLength clone() {
        return updateClone(new JvmObjectArrayLength(object.clone()));
    }

    @Override
    protected void appendTo(StringBuilder sb) {
        sb.append(object);
        sb.append(".length");
    }
}
