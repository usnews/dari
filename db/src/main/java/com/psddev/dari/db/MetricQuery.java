package com.psddev.dari.db;

class MetricQuery {
    private String symbol;
    private final State state;
    private Long startTimestamp;
    private Long endTimestamp;

    public MetricQuery(String symbol, State state) {
        this.symbol = symbol;
        this.state = state;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public State getState() {
        return state;
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
        return "action: " + getSymbol() + " recordId: " + getState().getId() + " date range: " + startTimestamp + " - " + endTimestamp;
    }
}
