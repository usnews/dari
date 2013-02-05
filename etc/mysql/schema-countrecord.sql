
-- mysql

DROP TABLE IF EXISTS CountRecord;
CREATE TABLE CountRecord (
    id BINARY(16) NOT NULL,
    typeSymbolId INT NOT NULL,
    actionSymbolId INT NOT NULL,
    amount INT NOT NULL,
    createDate INT NOT NULL,
    updateDate INT NOT NULL,
    eventDate INT NOT NULL,
    PRIMARY KEY (typeSymbolId, actionSymbolId, id, eventDate)
);

CREATE OR REPLACE VIEW CountRecord_d AS
SELECT hex(c.id) AS id
, ts.value as typeSymbol
, ls.value as actionSymbol
, amount
, FROM_UNIXTIME(createDate) as createDate
, FROM_UNIXTIME(updateDate) as updateDate
, FROM_UNIXTIME(eventDate) as eventDate
FROM CountRecord c
JOIN Symbol ts ON (c.typeSymbolId = ts.symbolId)
JOIN Symbol ls ON (c.actionSymbolId = ls.symbolId);

DROP TABLE IF EXISTS CountRecordString;
CREATE TABLE CountRecordString (
    id BINARY(16) NOT NULL,
    typeSymbolId INT NOT NULL,
    symbolId INT NOT NULL,
    value VARCHAR(500) NOT NULL,
    PRIMARY KEY (symbolId, value, typeSymbolId, id),
    KEY k_id (id)
);

CREATE OR REPLACE VIEW CountRecordString_d AS
SELECT hex(c.id) AS id
, ts.value as typeSymbol
, s.value as symbol
, c.value
FROM CountRecordString c
JOIN Symbol ts ON (c.typeSymbolId = ts.symbolId)
JOIN Symbol s ON (c.symbolId = s.symbolId);

DROP TABLE IF EXISTS CountRecordDouble;
CREATE TABLE CountRecordDouble (
    id BINARY(16) NOT NULL,
    typeSymbolId INT NOT NULL,
    symbolId INT NOT NULL,
    value DOUBLE NOT NULL,
    PRIMARY KEY (symbolId, value, typeSymbolId, id),
    KEY k_id (id)
);
CREATE OR REPLACE VIEW CountRecordDouble_d AS
SELECT hex(c.id) AS id
, ts.value as typeSymbol
, s.value as symbol
, c.value
FROM CountRecordDouble c
JOIN Symbol ts ON (c.typeSymbolId = ts.symbolId)
JOIN Symbol s ON (c.symbolId = s.symbolId);

DROP TABLE IF EXISTS CountRecordInteger;
CREATE TABLE CountRecordInteger (
    id BINARY(16) NOT NULL,
    typeSymbolId INT NOT NULL,
    symbolId INT NOT NULL,
    value INTEGER NOT NULL,
    PRIMARY KEY (symbolId, value, typeSymbolId, id),
    KEY k_id (id)
);
CREATE OR REPLACE VIEW CountRecordInteger_d AS
SELECT hex(c.id) AS id
, ts.value as typeSymbol
, s.value as symbol
, c.value
FROM CountRecordInteger c
JOIN Symbol ts ON (c.typeSymbolId = ts.symbolId)
JOIN Symbol s ON (c.symbolId = s.symbolId);

DROP TABLE IF EXISTS CountRecordUuid;
CREATE TABLE CountRecordUuid (
    id BINARY(16) NOT NULL,
    typeSymbolId INT NOT NULL,
    symbolId INT NOT NULL,
    value BINARY(16) NOT NULL,
    PRIMARY KEY (symbolId, value, typeSymbolId, id),
    KEY k_id (id)
);

CREATE OR REPLACE VIEW CountRecordUuid_d AS
SELECT hex(c.id) AS id
, ts.value as typeSymbol
, s.value as symbol
, hex(c.value) as value
FROM CountRecordUuid c
JOIN Symbol ts ON (c.typeSymbolId = ts.symbolId)
JOIN Symbol s ON (c.symbolId = s.symbolId);

DROP TABLE IF EXISTS CountRecordSummary;
CREATE TABLE CountRecordSummary (
    id binary(16) not null, 
    /*typeId binary(16) not null, XXX: needs feature/countperformance to work */
    symbolId int not null, 
    value int not null,
    PRIMARY KEY (symbolId, value, /*typeId, */id),
    KEY k_id (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;

CREATE OR REPLACE VIEW CountRecordSummary_d AS
SELECT hex(c.id) AS id
, s.value as symbol
, c.value
FROM CountRecordSummary c
JOIN Symbol s ON (c.symbolId = s.symbolId);

