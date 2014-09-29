package com.psddev.dari.db;

import java.lang.reflect.Method;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.StringUtils;
import com.psddev.dari.util.TypeDefinition;

/** Description of how field methods can be indexed in a state. */
@ObjectField.Embedded
public class ObjectMethod extends ObjectField {

    public static final String JAVA_METHOD_NAME_KEY = "java.method";

    @InternalName("java.method")
    private String javaMethodName;

    public ObjectMethod(ObjectMethod method) {
        super(method);
        javaMethodName = method.getJavaMethodName();
    }

    public ObjectMethod(ObjectStruct parent, Map<String, Object> definition) {
        super(parent, definition);
        if (definition == null) {
            return;
        }
        javaMethodName = (String) definition.remove(JAVA_METHOD_NAME_KEY);
    }

    public String getJavaMethodName() {
        return javaMethodName;
    }

    public void setJavaMethodName(String javaMethodName) {
        this.javaMethodName = javaMethodName;
    }

    public Method getJavaMethod(Class<?> objectClass) {
        if (getJavaMethodName() == null) {
            return null;
        }
        Class<?> declaringClass = ObjectUtils.getClassByName(getJavaDeclaringClassName());

        return declaringClass != null && declaringClass.isAssignableFrom(objectClass) ?
                javaMethodCache.getUnchecked(objectClass) :
                null;
    }

    private final transient LoadingCache<Class<?>, Method> javaMethodCache = CacheBuilder.
            newBuilder().
            build(new CacheLoader<Class<?>, Method>() {

        @Override
        public Method load(Class<?> objectClass) {
            return TypeDefinition.getInstance(objectClass).getMethod(getJavaMethodName());
        }
    });

    /** Returns the display name. */
    public String getDisplayName() {
        if (!ObjectUtils.isBlank(displayName)) {
            return displayName;
        }

        String name = getJavaMethodName();

        if (ObjectUtils.isBlank(name)) {
            name = getInternalName();
        }

        int dotAt = name.lastIndexOf('.');

        if (dotAt > -1) {
            name = name.substring(dotAt + 1);
        }

        int dollarAt = name.lastIndexOf('$');

        if (dollarAt > -1) {
            name = name.substring(dollarAt + 1);
        }

        Matcher nameMatcher = StringUtils.getMatcher(name, "^(get|(is|has))([^a-z])(.*)$");
        if (nameMatcher.matches()) {
            name = ObjectUtils.isBlank(nameMatcher.group(2)) ?
                nameMatcher.group(3).toLowerCase(Locale.ENGLISH) + nameMatcher.group(4) :
                name;
        }

        name = StringUtils.toLabel(name);

        if (!name.endsWith("?") &&
                BOOLEAN_TYPE.equals(getInternalItemType())) {
            name += "?";
        }

        return name;
    }

    public void updateIndex(State state) {
        if (state == null) {
            return;
        }
        Database db = state.getDatabase();

        for (ObjectIndex idx : state.getType().getIndexes()) {
            if (idx.getFields().contains(getInternalName())) {
                db.updateIndex(state, idx);
            }
        }

        for (ObjectIndex idx : db.getEnvironment().getIndexes()) {
            if (idx.getFields().contains(getInternalName())) {
                db.updateIndex(state, idx);
            }
        }

    }

}
