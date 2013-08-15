CREATE TABLE IF NOT EXISTS Record (
    id UUID NOT NULL,
    typeId UUID NOT NULL,
    data bytea NOT NULL,
    PRIMARY KEY (typeId, id)
);

CREATE INDEX k_record_id ON Record (id);

CREATE OR REPLACE VIEW Record_d AS SELECT id, typeId, CONVERT_FROM(data, 'UTF-8') AS data FROM Record;

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS RecordLocation2 (
    id UUID NOT NULL,
    symbolId INT NOT NULL,
    value GEOMETRY(POINT,4326) NOT NULL,
    PRIMARY KEY (symbolId, value, id)
);

CREATE INDEX k_recordlocation_id ON RecordLocation2 (id);

CREATE INDEX k_recordlocation_value_gix ON RecordLocation2 USING GIST ( value );

CREATE TABLE IF NOT EXISTS RecordNumber2 (
    id UUID NOT NULL,
    symbolId INT NOT NULL,
    value DECIMAL NOT NULL,
    PRIMARY KEY (symbolId, value, id)
);

CREATE INDEX k_recordnumber2_id ON RecordNumber2 (id);

CREATE TABLE IF NOT EXISTS RecordString3 (
    id UUID NOT NULL,
    symbolId INT NOT NULL,
    value bytea NOT NULL,
    PRIMARY KEY (symbolId, value, id)
);

CREATE INDEX k_recordstring3_id ON RecordString3 (id);

CREATE OR REPLACE VIEW RecordString3_d AS SELECT id, symbolId, CONVERT_FROM(value, 'UTF-8') AS value FROM RecordString3;

CREATE TABLE IF NOT EXISTS RecordUpdate (
    id UUID NOT NULL,
    typeId UUID NOT NULL,
    updateDate DECIMAL NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX k_recordstring3_typeId_updateDate ON RecordUpdate (typeId, updateDate);

CREATE INDEX k_recordstring3_updateDate ON RecordUpdate (updateDate);

CREATE TABLE IF NOT EXISTS RecordUuid2 (
    id UUID NOT NULL,
    symbolId INT NOT NULL,
    value UUID NOT NULL,
    PRIMARY KEY (symbolId, value, id)
);

CREATE INDEX k_recorduuid2_id ON RecordUuid2 (id);

CREATE SEQUENCE symbol_seq;

CREATE TABLE IF NOT EXISTS Symbol (
    symbolId INT NOT NULL DEFAULT NEXTVAL('symbol_seq'),
    value bytea NOT NULL,
    PRIMARY KEY (symbolId)
);

CREATE UNIQUE INDEX k_symbol_value ON Symbol (value);

CREATE OR REPLACE VIEW Symbol_d AS SELECT symbolId, CONVERT_FROM(value, 'UTF-8') AS value FROM Symbol;

CREATE TABLE IF NOT EXISTS Metric (
    id UUID NOT NULL,
    typeId UUID NOT NULL,
    symbolId INT NOT NULL,
    dimensionId UUID NOT NULL,
    data CHAR(40) NOT NULL,
    PRIMARY KEY (symbolId, typeId, id, dimensionId, data)
);

CREATE INDEX k_metricAllDims ON Metric (symbolId, typeId, id, data, dimensionId);

CREATE UNIQUE INDEX k_metricData ON Metric (symbolId, id, dimensionId, left(data, 8));

CREATE TABLE IF NOT EXISTS MetricDimension (
    dimensionId UUID NOT NULL PRIMARY KEY,
    value BYTEA NOT NULL
);

CREATE UNIQUE INDEX k_metricDimensionValue ON MetricDimension(value);

CREATE OR REPLACE VIEW Metric_n AS
SELECT c.id
, c.typeId
, c.symbolId
, c.dimensionId
, ROUND(('x'||SUBSTRING(data,25,16))::bit(64)::bigint / 1000000, 6)  AS amount
, ROUND(('x'||SUBSTRING(data,9,16))::bit(64)::bigint / 1000000, 6)  AS cumulativeAmount
, ('x'||SUBSTRING(data,1,8))::bit(32)::bigint * 60000 AS eventDate
, data AS data
FROM Metric c;

CREATE OR REPLACE VIEW Metric_d AS
SELECT c.id
, c.typeId
, c.symbolId
, c.dimensionId
, ENCODE(d.value, 'ESCAPE') AS dimension
, ENCODE(ls.value, 'ESCAPE') AS symbol
, ROUND(('x'||SUBSTRING(data,25,16))::bit(64)::bigint / 1000000, 6)  AS amount
, ROUND(('x'||SUBSTRING(data,9,16))::bit(64)::bigint / 1000000, 6)  AS cumulativeAmount
, TO_TIMESTAMP(('x'||SUBSTRING(data,1,8))::bit(32)::bigint * 60)::TIMESTAMP AS eventDate
, data
, ('x'||SUBSTRING(data,1,8))::bit(32)::bigint * 60000 AS eventTimestamp
FROM Metric c
JOIN Symbol ls ON (c.symbolId = ls.symbolId)
LEFT JOIN MetricDimension d ON (c.dimensionId = d.dimensionId);
