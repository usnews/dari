
CREATE TABLE IF NOT EXISTS Record (
    id BINARY(16) NOT NULL,
    typeId BINARY(16) NOT NULL,
    data LONGBLOB NOT NULL,
    PRIMARY KEY (typeId, id),
    KEY k_id (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin ROW_FORMAT=DYNAMIC;
CREATE VIEW Record_d AS SELECT hex(id) AS id, hex(typeId) AS typeId, DATA FROM Record;

CREATE TABLE IF NOT EXISTS RecordLocation3 (
    typeId BINARY(16) NOT NULL,
    id BINARY(16) NOT NULL,
    symbolId INT NOT NULL,
    value POINT NOT NULL,
    PRIMARY KEY (typeId, symbolId, value, id),
    KEY k_id (id),
    SPATIAL KEY k_value (value)
) ENGINE= MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_bin;
CREATE VIEW RecordLocation3_d AS SELECT hex(typeId) as typeId, hex(id) AS id, symbolId, VALUE FROM RecordLocation3;

CREATE TABLE IF NOT EXISTS RecordNumber3 (
    typeId BINARY(16) NOT NULL,
    id BINARY(16) NOT NULL,
    symbolId INT NOT NULL,
    value DOUBLE NOT NULL,
    PRIMARY KEY (typeId, symbolId, value, id),
    KEY k_id (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;
CREATE VIEW RecordNumber3_d AS SELECT hex(typeId) as typeId, hex(id) AS id, symbolId, VALUE FROM RecordNumber3;

CREATE TABLE IF NOT EXISTS RecordString4 (
    typeId BINARY(16) NOT NULL,
    id BINARY(16) NOT NULL,
    symbolId INT NOT NULL,
    value VARBINARY(500) NOT NULL,
    PRIMARY KEY (typeId, symbolId, value, id),
    KEY k_id (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;
CREATE VIEW RecordString4_d AS SELECT hex(typeId) AS typeId, hex(id) AS id, symbolId, VALUE FROM RecordString4;

CREATE TABLE IF NOT EXISTS RecordUpdate (
    id BINARY(16) NOT NULL,
    typeId BINARY(16) NOT NULL,
    updateDate DOUBLE NOT NULL,
    PRIMARY KEY (id),
    KEY k_typeId_updateDate (typeId, updateDate),
    KEY k_updateDate (updateDate)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;
CREATE VIEW RecordUpdate_d AS SELECT hex(id) AS id, hex(typeId) AS typeId, updateDate FROM RecordUpdate;

CREATE TABLE IF NOT EXISTS RecordUuid3 (
    typeId BINARY(16) NOT NULL,
    id BINARY(16) NOT NULL,
    symbolId INT NOT NULL,
    value BINARY(16) NOT NULL,
    PRIMARY KEY (typeId, symbolId, value, id),
    KEY k_id (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;
CREATE VIEW RecordUuid3_d AS SELECT hex(typeId) as typeId, hex(id) AS id, symbolId, hex(VALUE) AS VALUE FROM RecordUuid3;

CREATE TABLE IF NOT EXISTS Symbol (
    symbolId INT NOT NULL AUTO_INCREMENT,
    value VARBINARY(500) NOT NULL,
    PRIMARY KEY (symbolId),
    UNIQUE KEY k_value (value)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;
