package com.psddev.dari.db.shyiko;

import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.google.common.base.Charsets;
import com.psddev.dari.util.StringUtils;

public class DariQueryEventData extends QueryEventData {

    private static final long serialVersionUID = 1L;

    private byte[] statement;
    private Action action;
    private byte[] id;
    private byte[] typeId;
    private byte[] data;

    @Override
    public String getSql() {
        if (super.getSql() == null) {
            setSql(new String(statement, Charsets.US_ASCII));
        }
        return super.getSql();
    }

    public byte[] getStatement() {
        return statement;
    }

    public void setStatement(byte[] statement) {
        this.statement = statement;
    }

    public Action getAction() {
        return action;
    }

    public void setActionl(Action action) {
        this.action = action;
    }

    public byte[] getId() {
        return id;
    }

    public void setId(byte[] id) {
        this.id = id;
    }

    public byte[] getTypeId() {
        return typeId;
    }

    public void setTypeId(byte[] typeId) {
        this.typeId = typeId;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("DariQueryEventData");
        sb.append("{threadId=").append(getThreadId());
        sb.append(", executionTime=").append(getExecutionTime());
        sb.append(", errorCode=").append(getErrorCode());
        sb.append(", database='").append(getDatabase()).append('\'');
        sb.append(", sql='").append(getSql()).append('\'');
        sb.append(", statement='").append(getStatement()).append('\'');
        sb.append(", id='").append(StringUtils.hex(id)).append('\'');
        sb.append(", typeId='").append(StringUtils.hex(typeId)).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public enum Action {
        INSERT,
        UPDATE,
        DELETE;
    }
}
