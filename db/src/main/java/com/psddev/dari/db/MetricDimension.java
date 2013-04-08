package com.psddev.dari.db;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

class MetricDimension implements Comparable<MetricDimension> {

    private final ObjectField objectField;
    private Set<Object> values = new HashSet<Object>();

    public MetricDimension(ObjectField objectField) {
        this.objectField = objectField;
    }

    public String getSymbol() {
        return getKey();
    }

    public String getKey() {
        return objectField.getUniqueName();
    }

    public ObjectField getObjectField() {
        return objectField;
    }

    public Set<Object> getValues() {
        return values;
    }

    public void addValue(UUID value) {
        values.add(value);
    }

    public void addValue(String value) {
        values.add(value);
    }

    public void addValue(Number value) {
        values.add(value);
    }

    public void addValue(Object value) {
        values.add(value.toString());
    }

    public String getIndexTable () {
        return Metric.Static.getIndexTable(getObjectField());
    }

    public String toString() {
        StringBuilder str = new StringBuilder(getSymbol());
        if (values.size() > 1) {
            str.append("[");
            str.append(values.size());
            str.append("]");
        }
        return str.toString();
    }

    @Override
    public int compareTo(MetricDimension arg0) {
        return getSymbol().compareTo(arg0.getSymbol());
    }
}
