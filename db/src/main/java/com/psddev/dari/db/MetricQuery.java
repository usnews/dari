package com.psddev.dari.db;

import java.util.UUID;

class MetricQuery {
    private String symbol;
    private final Record record;
    private Long startTimestamp;
    private Long endTimestamp;

    public MetricQuery(String symbol, Record record) {
        this.symbol = symbol;
        this.record = record;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public Record getRecord() {
        return record;
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
        return "action: " + getSymbol() + " recordId: " + getRecord().getId() + " date range: " + startTimestamp + " - " + endTimestamp;
    }
}
