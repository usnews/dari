CREATE TABLE IF NOT EXISTS Record (
    id UUID NOT NULL,
    typeId UUID NOT NULL,
    data LONGVARBINARY NOT NULL,
    PRIMARY KEY (typeId, id)
);

CREATE INDEX IF NOT EXISTS k_Record_id ON Record (id);

CREATE TABLE IF NOT EXISTS RecordLocation2 (
    id UUID NOT NULL,
    symbolId INT NOT NULL,
    value DOUBLE NOT NULL,
    PRIMARY KEY (symbolId, value, id)
);

CREATE INDEX IF NOT EXISTS k_RecordLocation2_id ON RecordLocation2 (id);

CREATE TABLE IF NOT EXISTS RecordNumber2 (
    id UUID NOT NULL,
    symbolId INT NOT NULL,
    value DOUBLE NOT NULL,
    PRIMARY KEY (symbolId, value, id)
);

CREATE INDEX IF NOT EXISTS k_RecordNumber2_id ON RecordNumber2 (id);

CREATE TABLE IF NOT EXISTS RecordString3 (
    id UUID NOT NULL,
    symbolId INT NOT NULL,
    value VARBINARY(500) NOT NULL,
    PRIMARY KEY (symbolId, value, id)
);

CREATE INDEX IF NOT EXISTS k_RecordString3_id ON RecordString3 (id);

CREATE TABLE IF NOT EXISTS RecordUpdate (
    id UUID NOT NULL,
    typeId UUID NOT NULL,
    updateDate DOUBLE NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS k_RecordUpdate_typeId_updateDate ON RecordUpdate (typeId, updateDate);

CREATE INDEX IF NOT EXISTS k_RecordUpdate_updateDate ON RecordUpdate (updateDate);

CREATE TABLE IF NOT EXISTS RecordUuid2 (
    id UUID NOT NULL,
    symbolId INT NOT NULL,
    value UUID NOT NULL,
    PRIMARY KEY (symbolId, value, id)
);

CREATE INDEX IF NOT EXISTS k_RecordUuid2_id ON RecordUuid2 (id);

CREATE TABLE IF NOT EXISTS Symbol (
    symbolId INT NOT NULL AUTO_INCREMENT,
    value VARBINARY(500) NOT NULL,
    PRIMARY KEY (symbolId)
);

CREATE UNIQUE INDEX IF NOT EXISTS k_Symbol_value ON Symbol (value);
