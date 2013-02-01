
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

DROP TABLE IF EXISTS CountRecordDouble;
CREATE TABLE CountRecordDouble (
    id BINARY(16) NOT NULL,
    typeSymbolId INT NOT NULL,
    symbolId INT NOT NULL,
    value DOUBLE NOT NULL,
    PRIMARY KEY (symbolId, value, typeSymbolId, id),
    KEY k_id (id)
);

DROP TABLE IF EXISTS CountRecordInteger;
CREATE TABLE CountRecordInteger (
    id BINARY(16) NOT NULL,
    typeSymbolId INT NOT NULL,
    symbolId INT NOT NULL,
    value INTEGER NOT NULL,
    PRIMARY KEY (symbolId, value, typeSymbolId, id),
    KEY k_id (id)
);

DROP TABLE IF EXISTS CountRecordUuid;
CREATE TABLE CountRecordUuid (
    id BINARY(16) NOT NULL,
    typeSymbolId INT NOT NULL,
    symbolId INT NOT NULL,
    value BINARY(16) NOT NULL,
    PRIMARY KEY (symbolId, value, typeSymbolId, id),
    KEY k_id (id)
);

