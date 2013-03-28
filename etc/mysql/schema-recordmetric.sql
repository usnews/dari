
-- mysql

DROP TABLE IF EXISTS RecordMetric;
CREATE TABLE RecordMetric (
    metricId BINARY(16) NOT NULL,
    dimensionsSymbolId INT NOT NULL,
    actionSymbolId INT NOT NULL,
    createDate BIGINT NOT NULL,
    updateDate BIGINT NOT NULL,
    data BINARY(20) NOT NULL,
    PRIMARY KEY (actionSymbolId, metricId, data)
    /*,UNIQUE KEY k_dataEventDate (actionSymbolId, metricId, data(4))*/ /* This one can be removed in production; it's only there to ensure RecordMetric doesn't misbehave. */
) ENGINE=InnoDB DEFAULT CHARSET=binary;

CREATE OR REPLACE VIEW RecordMetric_d AS
SELECT hex(c.metricId) AS metricId
, ds.value as dimensionsSymbol
, ls.value as actionSymbol
, ROUND(CONV(HEX(SUBSTR(data, 13, 8)), 16, 10) / 1000000, 6) amount
, ROUND(CONV(HEX(SUBSTR(data, 5, 8)), 16, 10) / 1000000, 6) cumulativeAmount
, FROM_UNIXTIME(CONV(HEX(SUBSTR(data, 1, 4)), 16, 10) * 60) eventDate
, FROM_UNIXTIME(createDate/1000) as createDate
, FROM_UNIXTIME(updateDate/1000) as updateDate
, HEX(data) AS data
, CONV(HEX(SUBSTR(data, 1, 4)), 16, 10) * 60000 eventTimestamp
FROM RecordMetric c
JOIN Symbol ds ON (c.dimensionsSymbolId = ds.symbolId)
JOIN Symbol ls ON (c.actionSymbolId = ls.symbolId);

/* 

Simple query to get to the current metric for a given record + actionSymbol (this eliminates RecordMetricSummary) :

SELECT id, typeId, SUM(cumulativeAmount) as amount 
FROM (
    SELECT rcr.id id, rcr.typeId typeId
    , (CONV(HEX(SUBSTR(data, 1, 4)), 16, 10) * 60000) eventDate
    , ROUND(CONV(HEX(SUBSTR(MAX(data), 5, 8)), 16, 10) / 1000000, 6) cumulativeAmount
    FROM RecordRecordMetric rcr
    JOIN RecordMetric cr ON (rcr.metricId = cr.metricId)
    WHERE rcr.typeId = 0x0000013D26DEDF28A5BD67FFEF550010
    AND rcr.id = 0x0000013D6BBBD3E1AB7D6BBF3E3A0000
    AND cr.actionSymbolId = 13
    GROUP BY rcr.id, rcr.typeId, cr.metricId
) x
GROUP BY id, typeId

*/

DROP TABLE IF EXISTS RecordMetricString;
CREATE TABLE RecordMetricString (
    metricId BINARY(16) NOT NULL,
    dimensionsSymbolId INT NOT NULL,
    symbolId INT NOT NULL,
    value VARCHAR(500) NOT NULL,
    PRIMARY KEY (symbolId, value, dimensionsSymbolId, metricId),
    KEY k_metricId (metricId)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;

CREATE OR REPLACE VIEW RecordMetricString_d AS
SELECT hex(c.metricId) AS metricId
, ds.value as dimensionsSymbol
, s.value as symbol
, c.value
FROM RecordMetricString c
JOIN Symbol ds ON (c.dimensionsSymbolId = ds.symbolId)
JOIN Symbol s ON (c.symbolId = s.symbolId);

DROP TABLE IF EXISTS RecordMetricNumber;
CREATE TABLE RecordMetricNumber (
    metricId BINARY(16) NOT NULL,
    dimensionsSymbolId INT NOT NULL,
    symbolId INT NOT NULL,
    value DOUBLE NOT NULL,
    PRIMARY KEY (symbolId, value, dimensionsSymbolId, metricId),
    KEY k_metricId (metricId)
) ENGINE=InnoDB DEFAULT CHARSET=binary;

CREATE OR REPLACE VIEW RecordMetricNumber_d AS
SELECT hex(c.metricId) AS metricId
, ds.value as dimensionsSymbol
, s.value as symbol
, c.value
FROM RecordMetricNumber c
JOIN Symbol ds ON (c.dimensionsSymbolId = ds.symbolId)
JOIN Symbol s ON (c.symbolId = s.symbolId);

DROP TABLE IF EXISTS RecordMetricUuid;
CREATE TABLE RecordMetricUuid (
    metricId BINARY(16) NOT NULL,
    dimensionsSymbolId INT NOT NULL,
    symbolId INT NOT NULL,
    value BINARY(16) NOT NULL,
    PRIMARY KEY (symbolId, value, dimensionsSymbolId, metricId),
    KEY k_metricId (metricId)
) ENGINE=InnoDB DEFAULT CHARSET=binary;

CREATE OR REPLACE VIEW RecordMetricUuid_d AS
SELECT hex(c.metricId) AS metricId
, ds.value as dimensionsSymbol
, s.value as symbol
, hex(c.value) as value
FROM RecordMetricUuid c
JOIN Symbol ds ON (c.dimensionsSymbolId = ds.symbolId)
JOIN Symbol s ON (c.symbolId = s.symbolId);

DROP TABLE IF EXISTS RecordMetricLocation;
CREATE TABLE RecordMetricLocation (
    metricId BINARY(16) NOT NULL,
    dimensionsSymbolId INT NOT NULL,
    symbolId INT NOT NULL,
    value POINT NOT NULL,
    PRIMARY KEY (symbolId, value, dimensionsSymbolId, metricId),
    KEY k_metricId (metricId),
    SPATIAL KEY k_value (value)
) ENGINE=MyISAM DEFAULT CHARSET=binary;

CREATE OR REPLACE VIEW RecordMetricLocation_d AS
SELECT hex(c.metricId) AS metricId
, ds.value as dimensionsSymbol
, s.value as symbol
, astext(c.value) as value
FROM RecordMetricLocation c
JOIN Symbol ds ON (c.dimensionsSymbolId = ds.symbolId)
JOIN Symbol s ON (c.symbolId = s.symbolId);

DROP TABLE IF EXISTS RecordMetricRecord;
CREATE TABLE RecordMetricRecord (
    id BINARY(16) NOT NULL,
    typeId BINARY(16) NOT NULL,
    metricId BINARY(16) NOT NULL,
    PRIMARY KEY (typeId, id, metricId)
) ENGINE=InnoDB DEFAULT CHARSET=BINARY;

CREATE OR REPLACE VIEW RecordMetricRecord_d AS
SELECT hex(id) id
, hex(typeId) typeId
, hex(metricId) metricId
FROM RecordMetricRecord;

