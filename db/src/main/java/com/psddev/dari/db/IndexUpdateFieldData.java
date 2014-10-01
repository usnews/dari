package com.psddev.dari.db;

import com.psddev.dari.db.Recordable.FieldInternalNamePrefix;

@FieldInternalNamePrefix("dari.indexupdate.")
class IndexUpdateFieldData extends Modification<ObjectField> {

    private String delayClassName;
    private boolean immediate;
    private transient IndexUpdateDelay delay;

    public void setDelayClass(Class<? extends IndexUpdateDelay> delayClass) {

        this.delayClassName = delayClass != null ? delayClass.getName() : null;
        this.delay = null;

    }

    @SuppressWarnings("unchecked")
    public IndexUpdateDelay getIndexUpdateDelay() {
        if (delay == null) {
            if (delayClassName == null) {
                return null;
            } else {
                try {
                    Class<IndexUpdateDelay> cls = (Class<IndexUpdateDelay>) Class.forName(delayClassName);
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

}
