package com.psddev.dari.util.sa;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class JvmInvocation {

    private final AccessibleObject constructorOrMethod;
    private final List<JvmObject> arguments;

    protected JvmInvocation(AccessibleObject constructorOrMethod, List<JvmObject> arguments) {
        this.constructorOrMethod = constructorOrMethod;
        this.arguments = new ArrayList<JvmObject>();

        for (JvmObject argument : arguments) {
            this.arguments.add(argument.clone());
        }
    }

    public Object resolve(Object object) {
        if (object != null) {
            constructorOrMethod.setAccessible(true);

            List<Object> resolvedArguments = new ArrayList<Object>();

            for (JvmObject argument : arguments) {
                resolvedArguments.add(argument.resolve());
            }

            try {
                if (constructorOrMethod instanceof Constructor) {
                    return ((Constructor<?>) constructorOrMethod).newInstance(resolvedArguments.toArray(new Object[0]));

                } else if (constructorOrMethod instanceof Method) {
                    return ((Method) constructorOrMethod).invoke(object instanceof Class ? null : object, resolvedArguments.toArray(new Object[0]));
                }

            } catch (IllegalAccessException error) {
                return null;

            } catch (IllegalArgumentException error) {
                return null;

            } catch (InstantiationException error) {
                return null;

            } catch (InvocationTargetException error) {
                return null;
            }
        }

        return null;
    }

    public void appendTo(StringBuilder sb, String prefix) {
        if (constructorOrMethod instanceof Method) {
            sb.append(prefix);
            sb.append(((Method) constructorOrMethod).getName());
        }

        sb.append('(');

        if (!arguments.isEmpty()) {
            for (JvmObject argument : arguments) {
                sb.append(argument);
                sb.append(", ");
            }

            sb.setLength(sb.length() - 2);
        }

        sb.append(')');
    }
}
