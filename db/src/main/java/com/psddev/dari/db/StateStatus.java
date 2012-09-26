package com.psddev.dari.db;

public enum StateStatus {

    SAVED,
    DELETED,
    REFERENCE_ONLY;

    static {
        int index = 0;
        for (StateStatus status : values()) {
            status.flag = 1 << index;
            ++ index;
        }
    }

    private int flag;

    public int getFlag() {
        return flag;
    }
}
