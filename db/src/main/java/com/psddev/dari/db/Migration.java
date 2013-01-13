package com.psddev.dari.db;

public class Migration extends Modification<Object> {

    private static final String PREFIX = "dari.migration.";
    public static final String MIGRATION_STATUS_FIELD = PREFIX + "status";

    public static enum Status {
        FAILED,
        INGESTED,
        COMPLETE,
        INCOMPLETEDATA
    }

    @Indexed
    @InternalName(MIGRATION_STATUS_FIELD)
    private Status status;

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }
}