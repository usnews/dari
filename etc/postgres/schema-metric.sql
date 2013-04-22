
-- postgresql

DROP TABLE IF EXISTS Metric;
CREATE TABLE Metric (
    id UUID NOT NULL,
    typeId UUID NOT NULL,
    symbolId INT NOT NULL,
    dimensionId UUID NOT NULL,
    data CHAR(40) NOT NULL,
    PRIMARY KEY (symbolId, typeId, id, dimensionId, data)
);
CREATE INDEX k_metricAllDims ON Metric (symbolId, typeId, id, data, dimensionId);
CREATE UNIQUE INDEX k_metricData ON Metric (symbolId, id, dimensionId, left(data, 8));

DROP TABLE IF EXISTS MetricDimension;
CREATE TABLE MetricDimension (
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
