
-- mysql

DROP TABLE IF EXISTS CountRecord;
CREATE TABLE CountRecord (
    id BINARY(16) NULL,
    countId BINARY(16) NOT NULL,
    typeId BINARY(16) NOT NULL,
    dimensionsSymbolId INT NOT NULL,
    actionSymbolId INT NOT NULL,
    amount DOUBLE NOT NULL,
    createDate BIGINT NOT NULL,
    updateDate BIGINT NOT NULL,
    eventDate BIGINT NOT NULL,
    PRIMARY KEY (actionSymbolId, countId, eventDate),
    KEY k_dimensionsid (dimensionsSymbolId),
    KEY k_recordid (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin ROW_FORMAT=DYNAMIC;

CREATE OR REPLACE VIEW CountRecord_d AS
SELECT hex(c.id) as id
, hex(c.countId) AS countId
, hex(c.typeId) as typeId
, ds.value as dimensionsSymbol
, ls.value as actionSymbol
, amount
, FROM_UNIXTIME(createDate/1000) as createDate
, FROM_UNIXTIME(updateDate/1000) as updateDate
, FROM_UNIXTIME(eventDate/1000) as eventDate
FROM CountRecord c
JOIN Symbol ds ON (c.dimensionsSymbolId = ds.symbolId)
JOIN Symbol ls ON (c.actionSymbolId = ls.symbolId);

DROP TABLE IF EXISTS CountRecordString;
CREATE TABLE CountRecordString (
    id BINARY(16) NULL,
    countId BINARY(16) NOT NULL,
    dimensionsSymbolId INT NOT NULL,
    symbolId INT NOT NULL,
    value VARCHAR(500) NOT NULL,
    PRIMARY KEY (symbolId, value, dimensionsSymbolId, countId),
    KEY k_countId (countId),
    KEY k_id (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;

CREATE OR REPLACE VIEW CountRecordString_d AS
SELECT hex(c.id) as id
, hex(c.countId) AS countId
, ds.value as dimensionsSymbol
, s.value as symbol
, c.value
FROM CountRecordString c
JOIN Symbol ds ON (c.dimensionsSymbolId = ds.symbolId)
JOIN Symbol s ON (c.symbolId = s.symbolId);

DROP TABLE IF EXISTS CountRecordNumber;
CREATE TABLE CountRecordNumber (
    id BINARY(16) NULL,
    countId BINARY(16) NOT NULL,
    dimensionsSymbolId INT NOT NULL,
    symbolId INT NOT NULL,
    value DOUBLE NOT NULL,
    PRIMARY KEY (symbolId, value, dimensionsSymbolId, countId),
    KEY k_countId (countId),
    KEY k_id (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;
CREATE OR REPLACE VIEW CountRecordNumber_d AS
SELECT hex(c.id) as id
, hex(c.countId) AS countId
, ds.value as dimensionsSymbol
, s.value as symbol
, c.value
FROM CountRecordNumber c
JOIN Symbol ds ON (c.dimensionsSymbolId = ds.symbolId)
JOIN Symbol s ON (c.symbolId = s.symbolId);

DROP TABLE IF EXISTS CountRecordUuid;
CREATE TABLE CountRecordUuid (
    id BINARY(16) NULL,
    countId BINARY(16) NOT NULL,
    dimensionsSymbolId INT NOT NULL,
    symbolId INT NOT NULL,
    value BINARY(16) NOT NULL,
    PRIMARY KEY (symbolId, value, dimensionsSymbolId, countId),
    KEY k_countId (countId),
    KEY k_id (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;

CREATE OR REPLACE VIEW CountRecordUuid_d AS
SELECT hex(c.id) as id
, hex(c.countId) AS countId
, ds.value as dimensionsSymbol
, s.value as symbol
, hex(c.value) as value
FROM CountRecordUuid c
JOIN Symbol ds ON (c.dimensionsSymbolId = ds.symbolId)
JOIN Symbol s ON (c.symbolId = s.symbolId);

DROP TABLE IF EXISTS CountRecordLocation;
CREATE TABLE CountRecordLocation (
    id BINARY(16) NULL,
    countId BINARY(16) NOT NULL,
    dimensionsSymbolId INT NOT NULL,
    symbolId INT NOT NULL,
    value POINT NOT NULL,
    PRIMARY KEY (symbolId, value, dimensionsSymbolId, countId),
    KEY k_countId (countId),
    KEY k_id (id),
    SPATIAL KEY k_value (value)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_bin;

CREATE OR REPLACE VIEW CountRecordLocation_d AS
SELECT hex(c.id) as id
, hex(c.countId) AS countId
, ds.value as dimensionsSymbol
, s.value as symbol
, c.value
FROM CountRecordLocation c
JOIN Symbol ds ON (c.dimensionsSymbolId = ds.symbolId)
JOIN Symbol s ON (c.symbolId = s.symbolId);

DROP TABLE IF EXISTS CountRecordSummary;
CREATE TABLE CountRecordSummary (
    id binary(16) NOT NULL, 
    /*typeId binary(16) NOT NULL, XXX: needs feature/countperformance to work */
    symbolId int NOT NULL, 
    value double NOT NULL,
    PRIMARY KEY (symbolId, value, /*typeId, */id),
    KEY k_id (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;

CREATE OR REPLACE VIEW CountRecordSummary_d AS
SELECT hex(c.id) AS id
, s.value as symbol
, c.value
FROM CountRecordSummary c
JOIN Symbol s ON (c.symbolId = s.symbolId);

