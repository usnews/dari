package com.psddev.dari.db;

import java.util.Map;

public interface Ingestion {

    public String getObjectName();

    public void ingest(Map<String,Object> objectMap);

}