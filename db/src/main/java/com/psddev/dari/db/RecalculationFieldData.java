package com.psddev.dari.db;

import com.psddev.dari.db.Recordable.FieldInternalNamePrefix;

@FieldInternalNamePrefix("dari.recalculation.")
public class RecalculationFieldData extends Modification<ObjectMethod> {

    private String delayClassName;

    private boolean immediate;

    private String metricFieldName;

    private String group;

    private transient RecalculationDelay delay;

    public void setDelayClass(Class<? extends RecalculationDelay> delayClass) {

        this.delayClassName = delayClass != null ? delayClass.getName() : null;
        this.delay = null;

    }

    @SuppressWarnings("unchecked")
    public RecalculationDelay getRecalculationDelay() {
        if (delay == null) {
            if (delayClassName == null) {
                return null;
            } else {
                try {
                    Class<RecalculationDelay> cls = (Class<RecalculationDelay>) Class.forName(delayClassName);
                    delay = cls.newInstance();
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                } catch (InstantiationException e) {
                    throw new RuntimeException(e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return delay;
    }

    public String getDelayClassName() {
        return delayClassName;
    }

    public boolean isImmediate() {
        return immediate;
    }

    public void setImmediate(boolean immediate) {
        this.immediate = immediate;
    }

    public String getMetricFieldName() {
        return metricFieldName;
    }

    public void setMetricFieldName(String metricFieldName) {
        this.metricFieldName = metricFieldName;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }
}
