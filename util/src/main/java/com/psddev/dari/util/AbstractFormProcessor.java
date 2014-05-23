package com.psddev.dari.util;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

/**
 * Skeletal form processor implementation. A subclass must implement:
 *
 * <ul>
 * <li>{@link #doProcess}</li>
 * </ul>
 *
 * @deprecated No replacement.
 */
@Deprecated
public abstract class AbstractFormProcessor implements FormProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFormProcessor.class);

    private HttpServletRequest request;
    private HttpServletResponse response;

    /**
     * Returns the request.
     *
     * @return Never {@code null} within execution of {@link #process}.
     */
    public HttpServletRequest getRequest() {
        return request;
    }

    /**
     * Sets the request.
     *
     * @param request Can't be {@code null}.
     */
    public void setRequest(HttpServletRequest request) {
        ErrorUtils.errorIfNull(request, "request");

        this.request = request;
    }

    /**
     * Returns the response.
     *
     * @return Sometimes {@code null} within execution of {@link #process}.
     */
    public HttpServletResponse getResponse() {
        return response;
    }

    /**
     * Sets the response.
     *
     * @param response Can be {@code null}.
     */
    public void setResponse(HttpServletResponse response) {
        this.response = response;
    }

    /** Called to process the form. */
    protected abstract void doProcess();

    // --- FormProcessor support ---

    @Override
    public final Object process(HttpServletRequest request, HttpServletResponse response) throws IOException {
        setRequest(request);
        setResponse(response);

        Class<?> thisClass = getClass();
        Set<String> names = new HashSet<String>();

        names.add("request");

        // Try to use bean setters to set parameters.
        try {
            for (PropertyDescriptor desc : Introspector.getBeanInfo(thisClass).getPropertyDescriptors()) {
                Method writeMethod = desc.getWriteMethod();

                if (writeMethod != null) {
                    Type[] parameterTypes = writeMethod.getGenericParameterTypes();

                    if (parameterTypes.length == 1) {
                        String name = desc.getName();

                        if (!names.contains(name)) {
                            try {
                                writeMethod.setAccessible(true);
                                writeMethod.invoke(this, ObjectUtils.to(parameterTypes[0], request.getParameter(name)));
                                names.add(name);

                            } catch (IllegalAccessException error) {
                                throw new IllegalStateException(error);

                            } catch (InvocationTargetException error) {
                                throw Throwables.propagate(error);
                            }
                        }
                    }
                }
            }

        } catch (IntrospectionException error) {
            LOGGER.warn(String.format(
                    "Can't introspect [%s] for setters!", thisClass.getName()),
                    error);
        }

        // Otherwise, try to set directly on the private fields.
        for (Map.Entry<String, List<Field>> entry : TypeDefinition.getInstance(thisClass).getAllSerializableFields().entrySet()) {
            String name = entry.getKey();

            if (!names.contains(name)) {
                for (Field field : entry.getValue()) {
                    try {
                        field.setAccessible(true);
                        field.set(this, ObjectUtils.to(field.getGenericType(), request.getParameter(name)));
                    } catch (IllegalAccessException error) {
                        // This should never happen since #setAccessible has
                        // been called on the field.
                    }
                }
            }
        }

        doProcess();

        return this;
    }
}
