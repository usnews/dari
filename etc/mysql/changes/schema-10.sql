CREATE TABLE IF NOT EXISTS Record (
    id BINARY(16) NOT NULL,
    typeId BINARY(16) NOT NULL,
    data LONGTEXT NOT NULL,
    PRIMARY KEY (id),
    KEY k_typeId_id (typeId, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin ROW_FORMAT=DYNAMIC;

CREATE TABLE IF NOT EXISTS RecordLocation2 (
    id BINARY(16) NOT NULL,
    symbolId INT NOT NULL,
    value POINT NOT NULL,
    PRIMARY KEY (symbolId, value, id),
    KEY k_id (id),
    SPATIAL KEY k_value (value)
) ENGINE= MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_bin;

CREATE TABLE IF NOT EXISTS RecordNumber2 (
    id BINARY(16) NOT NULL,
    symbolId INT NOT NULL,
    value DOUBLE NOT NULL,
    PRIMARY KEY (symbolId, value, id),
    KEY k_id (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;

CREATE TABLE IF NOT EXISTS RecordString2 (
    id BINARY(16) NOT NULL,
    symbolId INT NOT NULL,
    value VARBINARY(500) NOT NULL,
    PRIMARY KEY (symbolId, value, id),
    KEY k_id (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;

CREATE TABLE IF NOT EXISTS RecordUpdate (
    id BINARY(16) NOT NULL,
    typeId BINARY(16) NOT NULL,
    updateDate DOUBLE NOT NULL,
    PRIMARY KEY (id),
    KEY k_typeId_updateDate (typeId, updateDate),
    KEY k_updateDate (updateDate)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;

CREATE TABLE IF NOT EXISTS RecordUuid2 (
    id BINARY(16) NOT NULL,
    symbolId INT NOT NULL,
    value BINARY(16) NOT NULL,
    PRIMARY KEY (symbolId, value, id),
    KEY k_id (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;

CREATE TABLE IF NOT EXISTS Symbol (
    symbolId INT NOT NULL AUTO_INCREMENT,
    value VARBINARY(500) NOT NULL,
    PRIMARY KEY (symbolId),
    UNIQUE KEY k_value (value)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;
