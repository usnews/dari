package com.psddev.dari.db;

import java.util.UUID;

class MetricQuery {
    private final int symbolId;
    private final UUID id;
    private final UUID typeId;
    private Long startTimestamp;
    private Long endTimestamp;

    public MetricQuery(int symbolId, UUID id, UUID typeId) {
        this.symbolId = symbolId;
        this.id = id;
        this.typeId = typeId;
    }

    public int getSymbolId() {
        return symbolId;
    }

    public UUID getId() {
        return id;
    }

    public UUID getTypeId() {
        return typeId;
    }

    public Long getStartTimestamp() {
        return startTimestamp;
    }

    public Long getEndTimestamp() {
        return endTimestamp;
    }

    public void setDateRange(Long startTimestamp, Long endTimestamp) {
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
    }

    public String toString() {
        return "symbolId: " + getSymbolId() + " id: " + getId() + " date range: " + startTimestamp + " - " + endTimestamp;
    }
}
