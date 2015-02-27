package com.psddev.dari.util.sa;

import java.lang.reflect.Method;
import java.util.Map;

import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.LabelNode;
import org.objectweb.asm.tree.MethodNode;

import com.psddev.dari.util.CompactMap;

class JvmMethodVisitor extends MethodNode {

    private final Jvm jvm;
    private final Method callingMethod;
    private final Map<Integer, Type> localTypes = new CompactMap<Integer, Type>();
    private final Map<Integer, String> localNames = new CompactMap<Integer, String>();

    public JvmMethodVisitor(
            int access,
            String name,
            String desc,
            String signature,
            String[] exceptions,
            Jvm jvm,
            Method callingMethod) {

        super(access, name, desc, signature, exceptions);

        this.jvm = jvm;
        this.callingMethod = callingMethod;
    }

    public Jvm getJvm() {
        return jvm;
    }

    public Method getCallingMethod() {
        return callingMethod;
    }

    public Type getLocalType(int index) {
        return localTypes.get(index);
    }

    public String getLocalName(int index) {
        String name = localNames.get(index);

        return name != null ? name : "arg" + index;
    }

    @Override
    public LabelNode getLabelNode(Label label) {
        return super.getLabelNode(label);
    }

    @Override
    public void visitLocalVariable(String name, String desc, String signature, Label start, Label end, int index) {
        super.visitLocalVariable(name, desc, signature, start, end, index);

        localTypes.put(index, Type.getType(desc));
        localNames.put(index, name);
    }

    @Override
    public void visitEnd() {
        new JvmRunner(this).run();
    }
}
