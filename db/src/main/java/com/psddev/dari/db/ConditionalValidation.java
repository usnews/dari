package com.psddev.dari.db;

public interface ConditionalValidation extends Recordable {
    boolean shouldValidate(ObjectField field);
}
