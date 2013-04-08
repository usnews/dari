package com.psddev.dari.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

class MetricDimensionSet extends LinkedHashSet<MetricDimension> {
    private static final long serialVersionUID = 1L;

    public MetricDimensionSet(Set<MetricDimension> dimensions) {
        super(dimensions);
    }

    public MetricDimensionSet() {
        super();
    }

    public Set<String> keySet() {
        LinkedHashSet<String> keys = new LinkedHashSet<String>();
        for (MetricDimension d : this) {
            keys.add(d.getKey());
        }
        return keys;
    }

    public static MetricDimensionSet createDimensionSet(Set<ObjectField> dimensions, Record record) {
        LinkedHashSet<MetricDimension> dimensionSet = new LinkedHashSet<MetricDimension>();
        for (ObjectField field : dimensions) {
            LinkedHashSet<Object> values = new LinkedHashSet<Object>();
            Object value = record.getState().get(field.getInternalName());
            if (value == null) continue;
            if (value instanceof Set) {
                if (((Set<?>)value).size() == 0) continue;
                values.addAll((Set<?>)value);
            } else {
                values.add(value);
            }
            MetricDimension dim = new MetricDimension(field);
            for (Object val : values) {
                if (val instanceof UUID) {
                    dim.addValue((UUID) val);
                } else if (value instanceof Number) {
                    dim.addValue((Number) val);
                } else {
                    dim.addValue(val.toString());
                }
            }
            dimensionSet.add(dim);
        }
        return new MetricDimensionSet(dimensionSet);
    }

    public String getSymbol() {
        StringBuilder symbolBuilder = new StringBuilder();
        // if there is ever a prefix, put it here.
        //StringBuilder symbolBuilder = new StringBuilder(objectClass.getName());
        //symbolBuilder.append("/");

        boolean usedThisPrefix = false;
        String thisPrefix = "";
        for (MetricDimension d : getSortedDimensions()) {
            String dimSymbol = d.getSymbol();
            String prefix = dimSymbol.split("/")[0];
            if (! prefix.equals(thisPrefix)) {
                usedThisPrefix = false;
                thisPrefix = prefix;
            }
            if (!usedThisPrefix) {
                symbolBuilder.append(thisPrefix);
                symbolBuilder.append("/");
                usedThisPrefix = true;
            }
            if (dimSymbol.indexOf('/') > -1) {
                dimSymbol = dimSymbol.split("/")[1];
            }

            symbolBuilder.append(dimSymbol);
            if (d.getValues().size() > 1) {
                symbolBuilder.append("[");
                symbolBuilder.append(d.getValues().size());
                symbolBuilder.append("]");
            }
            symbolBuilder.append(',');
        }
        if (symbolBuilder.length() > 0) {
            symbolBuilder.setLength(symbolBuilder.length()-1);
        }
        symbolBuilder.append("#metric");
        return symbolBuilder.toString();
    }

    public String toString() {
        StringBuilder str = new StringBuilder(getSymbol());
        str.append(": ");
        for (MetricDimension dimension : this) {
            str.append(dimension.toString());
            str.append("=");
            str.append(dimension.getValues().toString());
            str.append(",");
        }
        str.setLength(str.length()-1);
        return str.toString();
    }

    private List<MetricDimension> getSortedDimensions() {
        ArrayList<MetricDimension> dims = new ArrayList<MetricDimension>(size());
        for (MetricDimension d : this) {
            dims.add(d);
        }
        Collections.sort(dims);
        return dims;
    }
}
