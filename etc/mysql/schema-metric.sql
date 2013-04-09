
-- mysql

DROP TABLE IF EXISTS Metric;
CREATE TABLE Metric (
    id BINARY(16) NOT NULL,
    typeId BINARY(16) NOT NULL,
    symbolId INT NOT NULL,
    createDate BIGINT NOT NULL,
    updateDate BIGINT NOT NULL,
    data BINARY(20) NOT NULL,
    PRIMARY KEY (symbolId, typeId, id, data)
    /*,UNIQUE KEY k_dataEventDate (actionSymbolId, metricId, data(4))*/ /* This one can be removed in production; it's only there to ensure Metric doesn't misbehave. */
) ENGINE=InnoDB DEFAULT CHARSET=binary;

CREATE OR REPLACE VIEW Metric_n AS
SELECT c.id
, c.typeId
, c.symbolId
, ROUND(CONV(HEX(SUBSTR(data, 13, 8)), 16, 10) / 1000000, 6) amount
, ROUND(CONV(HEX(SUBSTR(data, 5, 8)), 16, 10) / 1000000, 6) cumulativeAmount
, CONV(HEX(SUBSTR(data, 1, 4)), 16, 10) * 60000 eventDate
, createDate as createDate
, updateDate as updateDate
, data AS data
FROM Metric c;

CREATE OR REPLACE VIEW Metric_d AS
SELECT hex(c.id) AS id
, hex(c.typeId) as typeId
, c.symbolId
, ls.value as symbol
, ROUND(CONV(HEX(SUBSTR(data, 13, 8)), 16, 10) / 1000000, 6) amount
, ROUND(CONV(HEX(SUBSTR(data, 5, 8)), 16, 10) / 1000000, 6) cumulativeAmount
, FROM_UNIXTIME(CONV(HEX(SUBSTR(data, 1, 4)), 16, 10) * 60) eventDate
, FROM_UNIXTIME(createDate/1000) as createDate
, FROM_UNIXTIME(updateDate/1000) as updateDate
, HEX(data) AS data
, CONV(HEX(SUBSTR(data, 1, 4)), 16, 10) * 60000 eventTimestamp
FROM Metric c
JOIN Symbol ls ON (c.symbolId = ls.symbolId);

