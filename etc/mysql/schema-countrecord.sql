
-- mysql

DROP TABLE IF EXISTS CountRecord;
CREATE TABLE CountRecord (
    countId BINARY(16) NOT NULL,
    dimensionsSymbolId INT NOT NULL,
    actionSymbolId INT NOT NULL,
    createDate BIGINT NOT NULL,
    updateDate BIGINT NOT NULL,
    data BINARY(20) NOT NULL,
    PRIMARY KEY (actionSymbolId, countId, data)
    /*,UNIQUE KEY k_dataEventDate (actionSymbolId, countId, data(4))*/ /* This one can be removed in production; it's only there to ensure CountRecord doesn't misbehave. */
) ENGINE=InnoDB DEFAULT CHARSET=binary;

CREATE OR REPLACE VIEW CountRecord_d AS
SELECT hex(c.countId) AS countId
, ds.value as dimensionsSymbol
, ls.value as actionSymbol
, ROUND(CONV(HEX(SUBSTR(data, 13, 8)), 16, 10) / 1000000, 6) amount
, ROUND(CONV(HEX(SUBSTR(data, 5, 8)), 16, 10) / 1000000, 6) cumulativeAmount
, FROM_UNIXTIME(CONV(HEX(SUBSTR(data, 1, 4)), 16, 10) * 60) eventDate
, FROM_UNIXTIME(createDate/1000) as createDate
, FROM_UNIXTIME(updateDate/1000) as updateDate
, HEX(data) AS data
, CONV(HEX(SUBSTR(data, 1, 4)), 16, 10) * 60000 eventTimestamp
FROM CountRecord c
JOIN Symbol ds ON (c.dimensionsSymbolId = ds.symbolId)
JOIN Symbol ls ON (c.actionSymbolId = ls.symbolId);

/* 

Simple query to get to the current count for a given record + actionSymbol (this eliminates CountRecordSummary) :

SELECT id, typeId, SUM(cumulativeAmount) as amount 
FROM (
    SELECT rcr.id id, rcr.typeId typeId
    , (CONV(HEX(SUBSTR(data, 1, 4)), 16, 10) * 60000) eventDate
    , ROUND(CONV(HEX(SUBSTR(MAX(data), 5, 8)), 16, 10) / 1000000, 6) cumulativeAmount
    FROM RecordCountRecord rcr
    JOIN CountRecord cr ON (rcr.countId = cr.countId)
    WHERE rcr.typeId = 0x0000013D26DEDF28A5BD67FFEF550010
    AND rcr.id = 0x0000013D6BBBD3E1AB7D6BBF3E3A0000
    AND cr.actionSymbolId = 13
    GROUP BY rcr.id, rcr.typeId, cr.countId
) x
GROUP BY id, typeId

*/

DROP TABLE IF EXISTS CountRecordString;
CREATE TABLE CountRecordString (
    countId BINARY(16) NOT NULL,
    dimensionsSymbolId INT NOT NULL,
    symbolId INT NOT NULL,
    value VARCHAR(500) NOT NULL,
    PRIMARY KEY (symbolId, value, dimensionsSymbolId, countId),
    KEY k_countId (countId)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_general_ci;

CREATE OR REPLACE VIEW CountRecordString_d AS
SELECT hex(c.countId) AS countId
, ds.value as dimensionsSymbol
, s.value as symbol
, c.value
FROM CountRecordString c
JOIN Symbol ds ON (c.dimensionsSymbolId = ds.symbolId)
JOIN Symbol s ON (c.symbolId = s.symbolId);

DROP TABLE IF EXISTS CountRecordNumber;
CREATE TABLE CountRecordNumber (
    countId BINARY(16) NOT NULL,
    dimensionsSymbolId INT NOT NULL,
    symbolId INT NOT NULL,
    value DOUBLE NOT NULL,
    PRIMARY KEY (symbolId, value, dimensionsSymbolId, countId),
    KEY k_countId (countId)
) ENGINE=InnoDB DEFAULT CHARSET=binary;

CREATE OR REPLACE VIEW CountRecordNumber_d AS
SELECT hex(c.countId) AS countId
, ds.value as dimensionsSymbol
, s.value as symbol
, c.value
FROM CountRecordNumber c
JOIN Symbol ds ON (c.dimensionsSymbolId = ds.symbolId)
JOIN Symbol s ON (c.symbolId = s.symbolId);

DROP TABLE IF EXISTS CountRecordUuid;
CREATE TABLE CountRecordUuid (
    countId BINARY(16) NOT NULL,
    dimensionsSymbolId INT NOT NULL,
    symbolId INT NOT NULL,
    value BINARY(16) NOT NULL,
    PRIMARY KEY (symbolId, value, dimensionsSymbolId, countId),
    KEY k_countId (countId)
) ENGINE=InnoDB DEFAULT CHARSET=binary;

CREATE OR REPLACE VIEW CountRecordUuid_d AS
SELECT hex(c.countId) AS countId
, ds.value as dimensionsSymbol
, s.value as symbol
, hex(c.value) as value
FROM CountRecordUuid c
JOIN Symbol ds ON (c.dimensionsSymbolId = ds.symbolId)
JOIN Symbol s ON (c.symbolId = s.symbolId);

DROP TABLE IF EXISTS CountRecordLocation;
CREATE TABLE CountRecordLocation (
    countId BINARY(16) NOT NULL,
    dimensionsSymbolId INT NOT NULL,
    symbolId INT NOT NULL,
    value POINT NOT NULL,
    PRIMARY KEY (symbolId, value, dimensionsSymbolId, countId),
    KEY k_countId (countId),
    SPATIAL KEY k_value (value)
) ENGINE=MyISAM DEFAULT CHARSET=binary;

CREATE OR REPLACE VIEW CountRecordLocation_d AS
SELECT hex(c.countId) AS countId
, ds.value as dimensionsSymbol
, s.value as symbol
, astext(c.value) as value
FROM CountRecordLocation c
JOIN Symbol ds ON (c.dimensionsSymbolId = ds.symbolId)
JOIN Symbol s ON (c.symbolId = s.symbolId);

DROP TABLE IF EXISTS RecordCountRecord;
CREATE TABLE RecordCountRecord (
    id BINARY(16) NOT NULL,
    typeId BINARY(16) NOT NULL,
    countId BINARY(16) NOT NULL,
    PRIMARY KEY (typeId, id, countId)
) ENGINE=InnoDB DEFAULT CHARSET=BINARY;

CREATE OR REPLACE VIEW RecordCountRecord_d AS
SELECT hex(id) id
, hex(typeId) typeId
, hex(countId) countId
FROM RecordCountRecord;

