
-- mysql

DROP TABLE IF EXISTS CountRecord;
CREATE TABLE CountRecord (
    id BINARY(16) NOT NULL,
    typeSymbolId INT NOT NULL,
    amount INT NOT NULL,
    createDate INT NOT NULL,
    updateDate INT NOT NULL,
    eventDate INT NOT NULL,
    PRIMARY KEY (typeSymbolId, id, eventDate)
);

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

