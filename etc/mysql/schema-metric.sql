
-- mysql

DROP TABLE IF EXISTS Metric;
CREATE TABLE Metric (
    id BINARY(16) NOT NULL,
    typeId BINARY(16) NOT NULL,
    symbolId INT NOT NULL,
    dimensionId BINARY(16) NOT NULL,
    data BINARY(20) NOT NULL,
    PRIMARY KEY (symbolId, id, dimensionId, data(4)),
    KEY k_metricAllDims (symbolId, typeId, id, data, dimensionId),
    KEY k_metricData (symbolId, typeId, id, dimensionId, data)
) ENGINE=InnoDB DEFAULT CHARSET=binary;

DROP TABLE IF EXISTS MetricDimension;
CREATE TABLE MetricDimension (
    dimensionId BINARY(16) NOT NULL PRIMARY KEY,
    value VARBINARY(500) NOT NULL,
    UNIQUE KEY k_metricDimensionValue (value)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin ROW_FORMAT=DYNAMIC;

CREATE OR REPLACE VIEW Metric_n AS
SELECT c.id
, c.typeId
, c.symbolId
, c.dimensionId
, ROUND(CONV(HEX(SUBSTR(data, 13, 8)), 16, 10) / 1000000, 6) amount
, ROUND(CONV(HEX(SUBSTR(data, 5, 8)), 16, 10) / 1000000, 6) cumulativeAmount
, CONV(HEX(SUBSTR(data, 1, 4)), 16, 10) * 60000 eventDate
, data AS data
FROM Metric c;

CREATE OR REPLACE VIEW Metric_d AS
SELECT hex(c.id) AS id
, hex(c.typeId) as typeId
, c.symbolId
, hex(c.dimensionId) as dimensionId
, d.value AS dimension
, ls.value as symbol
, ROUND(CONV(HEX(SUBSTR(data, 13, 8)), 16, 10) / 1000000, 6) amount
, ROUND(CONV(HEX(SUBSTR(data, 5, 8)), 16, 10) / 1000000, 6) cumulativeAmount
, FROM_UNIXTIME(CONV(HEX(SUBSTR(data, 1, 4)), 16, 10) * 60) eventDate
, HEX(data) AS data
, CONV(HEX(SUBSTR(data, 1, 4)), 16, 10) * 60000 eventTimestamp
FROM Metric c
JOIN Symbol ls ON (c.symbolId = ls.symbolId)
LEFT JOIN MetricDimension d ON (c.dimensionId = d.dimensionId);

