package com.psddev.dari.db;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Internal annotation to mark that the fields in the target type
 * are {@linkplain LazyLoadEnhancer lazily loaded}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface LazyLoad {
}
