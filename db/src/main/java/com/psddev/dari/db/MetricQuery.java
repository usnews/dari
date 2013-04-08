package com.psddev.dari.db;

import java.util.UUID;

class MetricQuery {
    private final String symbol;
    private final String actionSymbol;
    private final MetricDimensionSet dimensions;
    private final Record record;
    private Long startTimestamp;
    private Long endTimestamp;
    private MetricDimensionSet groupByDimensions;
    private String[] orderByDimensions;
    private boolean includeSelfDimension;

    public MetricQuery(String symbol, String actionSymbol, MetricDimensionSet dimensions) {
        this.symbol = symbol;
        this.actionSymbol = actionSymbol;
        this.dimensions = dimensions;
        this.record = null;
    }

    public MetricQuery(String symbol, String actionSymbol, Record record,
            MetricDimensionSet dimensions) {
        this.symbol = symbol;
        this.actionSymbol = actionSymbol;
        this.dimensions = dimensions;
        this.record = record;
    }

    public UUID getRecordIdForInsert() {
        if (isIncludeSelfDimension()) {
            return record.getId();
        } else {
            return null;
        }
    }

    public boolean isIncludeSelfDimension() {
        return includeSelfDimension;
    }

    public void setIncludeSelfDimension(boolean includeSelfDimension) {
        this.includeSelfDimension = includeSelfDimension;
    }

    public void setOrderByDimensions(String[] orderByDimensions) {
        this.orderByDimensions = orderByDimensions;
    }

    public void setGroupByDimensions(MetricDimensionSet groupByDimensions) {
        this.groupByDimensions = groupByDimensions;
    }

    public String getSymbol() {
        return symbol;
    }

    public String getActionSymbol() {
        return actionSymbol;
    }

    public MetricDimensionSet getDimensions() {
        return dimensions;
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

    public MetricDimensionSet getGroupByDimensions() {
        return groupByDimensions;
    }

    public String[] getOrderByDimensions() {
        return orderByDimensions;
    }

    public String toString() {
        return "action: " + getActionSymbol() + " recordId: " + getRecordIdForInsert() + " date range: " + startTimestamp + " - " + endTimestamp + " dimensions: " + dimensions;
    }
}
