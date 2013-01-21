CREATE TABLE IF NOT EXISTS Record (
    id UUID NOT NULL,
    typeId UUID NOT NULL,
    data bytea NOT NULL,
    PRIMARY KEY (typeId, id)
);
CREATE INDEX k_record_id ON Record (id);
CREATE VIEW Record_d AS SELECT id, typeId, CONVERT_FROM(data, 'UTF-8') AS data FROM Record;

-- CREATE EXTENSION postgis;
CREATE TABLE IF NOT EXISTS RecordLocation3 (
    id UUID NOT NULL,
    typeId UUID NOT NULL,
    symbolId INT NOT NULL,
    value GEOMETRY(POINT,4326) NOT NULL,
    PRIMARY KEY (symbolId, value, typeId, id)
);
CREATE INDEX k_recordlocation_id ON RecordLocation3 (id);
CREATE INDEX k_recordlocation_value_gix ON RecordLocation3 USING GIST ( value );

CREATE TABLE IF NOT EXISTS RecordNumber3 (
    id UUID NOT NULL,
    typeId UUID NOT NULL,
    symbolId INT NOT NULL,
    value DECIMAL NOT NULL,
    PRIMARY KEY (symbolId, value, typeId, id)
);
CREATE INDEX k_recordnumber3_id ON RecordNumber3 (id);

CREATE TABLE IF NOT EXISTS RecordString4 (
    id UUID NOT NULL,
    typeId UUID NOT NULL,
    symbolId INT NOT NULL,
    value bytea NOT NULL,
    PRIMARY KEY (symbolId, value, typeId, id)
);
CREATE INDEX k_recordstring4_id ON RecordString4 (id);
CREATE VIEW RecordString4_d AS SELECT id, typeId, symbolId, CONVERT_FROM(value, 'UTF-8') AS value FROM RecordString4;

CREATE TABLE IF NOT EXISTS RecordUpdate (
    id UUID NOT NULL,
    typeId UUID NOT NULL,
    updateDate DECIMAL NOT NULL,
    PRIMARY KEY (id)
);
CREATE INDEX k_recordupdate_typeId_updateDate ON RecordUpdate (typeId, updateDate);
CREATE INDEX k_recordupdate_updateDate ON RecordUpdate (updateDate);

CREATE TABLE IF NOT EXISTS RecordUuid3 (
    id UUID NOT NULL,
    typeId UUID NOT NULL,
    symbolId INT NOT NULL,
    value UUID NOT NULL,
    PRIMARY KEY (symbolId, value, typeId, id)
);
CREATE INDEX k_recorduuid3_id ON RecordUuid3 (id);

CREATE SEQUENCE symbol_seq;
CREATE TABLE IF NOT EXISTS Symbol (
    symbolId INT NOT NULL DEFAULT NEXTVAL('symbol_seq'),
    value bytea NOT NULL,
    PRIMARY KEY (symbolId)
);
CREATE UNIQUE INDEX k_symbol_value ON Symbol (value);
CREATE VIEW Symbol_d AS SELECT symbolId, CONVERT_FROM(value, 'UTF-8') AS value FROM Symbol;
