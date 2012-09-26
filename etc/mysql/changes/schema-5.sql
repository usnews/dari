DROP TABLE IF EXISTS Record;
CREATE TABLE Record (
    id VARBINARY(16) NOT NULL,
    typeId VARBINARY(16) NOT NULL,
    data LONGTEXT NOT NULL,
    createDate BIGINT(20) NOT NULL,
    createUserId VARBINARY(16) NOT NULL,
    updateDate BIGINT(20) NOT NULL,
    updateUserId VARBINARY(16) NOT NULL,
    deleteDate BIGINT(20) NULL,
    deleteUserId VARBINARY(16) NULL,
    PRIMARY KEY (id),
    KEY k_typeId_updateDate (typeId, updateDate),
    KEY k_updateDate (updateDate),
    KEY k_updateUserId_updateDate (updateUserId, updateDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS RecordHistory;
CREATE TABLE RecordHistory (
    recordId VARBINARY(16) NOT NULL,
    id VARBINARY(16) NOT NULL,
    typeId VARBINARY(16) NOT NULL,
    data LONGTEXT NOT NULL,
    createDate BIGINT(20) NOT NULL,
    createUserId VARBINARY(16) NOT NULL,
    PRIMARY KEY (id),
    KEY k_recordId_createDate (recordId, createDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS RecordNumber;
CREATE TABLE RecordNumber (
    recordId VARBINARY(16) NOT NULL,
    name VARCHAR(20) NOT NULL,
    value DOUBLE NOT NULL,
    createDate BIGINT(20) NOT NULL,
    KEY k_recordId (recordId),
    KEY k_name_value (name, value),
    KEY k_name_createDate (name, value)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS RecordOutput;
CREATE TABLE RecordOutput (
    recordId VARBINARY(16) NOT NULL,
    format VARCHAR(20) NOT NULL,
    data LONGTEXT NOT NULL,
    createDate BIGINT(20) NOT NULL,
    PRIMARY KEY (recordId, format)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS RecordReference;
CREATE TABLE RecordReference (
    recordId VARBINARY(16) NOT NULL,
    typeId VARBINARY(16) NOT NULL,
    name VARCHAR(50) NOT NULL,
    position VARCHAR(20) NOT NULL,
    itemId VARBINARY(16) NOT NULL,
    KEY k_recordId_name_position (recordId, name, position),
    KEY k_typeId_name_itemId (typeId, name, itemId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS RecordString;
CREATE TABLE RecordString (
    recordId VARBINARY(16) NOT NULL,
    name VARCHAR(20) NOT NULL,
    value VARCHAR(200) NOT NULL,
    createDate BIGINT(20) NOT NULL,
    KEY k_recordId (recordId),
    KEY k_name_value (name, value),
    KEY k_name_createDate (name, createDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
