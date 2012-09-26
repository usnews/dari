DROP TABLE IF EXISTS Record;
CREATE TABLE Record (
    id BINARY(16) NOT NULL,
    typeId BINARY(16) NOT NULL,
    data LONGTEXT NOT NULL,
    PRIMARY KEY (id),
    KEY k_typeId (typeId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS RecordLocation;
CREATE TABLE RecordLocation (
    recordId BINARY(16) NOT NULL,
    name VARCHAR(100) NOT NULL,
    value POINT NOT NULL,
    KEY k_recordId_name (recordId, name),
    SPATIAL KEY k_value (value)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS RecordNumber;
CREATE TABLE RecordNumber (
    recordId BINARY(16) NOT NULL,
    name VARCHAR(100) NOT NULL,
    value DOUBLE NOT NULL,
    KEY k_recordId_name (recordId, name),
    KEY k_name_value (name, value)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS RecordString;
CREATE TABLE RecordString (
    recordId BINARY(16) NOT NULL,
    name VARCHAR(100) NOT NULL,
    value VARCHAR(400) NOT NULL,
    KEY k_recordId_name (recordId, name),
    KEY k_name_value (name, value)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS RecordUpdate;
CREATE TABLE RecordUpdate (
    id BINARY(16) NOT NULL,
    typeId BINARY(16) NOT NULL,
    updateDate DOUBLE NOT NULL,
    PRIMARY KEY (id),
    KEY k_typeId_updateDate (typeId, updateDate),
    KEY k_updateDate (updateDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS RecordUuid;
CREATE TABLE RecordUuid (
    recordId BINARY(16) NOT NULL,
    name VARCHAR(100) NOT NULL,
    value BINARY(16) NOT NULL,
    KEY k_recordId_name (recordId, name),
    KEY k_name_value (name, value)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;
