DROP TABLE IF EXISTS Record;
CREATE TABLE Record (
    id BIGINT(20) NOT NULL AUTO_INCREMENT,
    type VARCHAR(20) NOT NULL,
    data LONGTEXT NOT NULL,
    createDate BIGINT(20) NOT NULL,
    createUserId BIGINT(20) NOT NULL,
    updateDate BIGINT(20) NOT NULL,
    updateUserId BIGINT(20) NOT NULL,
    deleteDate BIGINT(20) NULL,
    deleteUserId BIGINT(20) NOT NULL,
    PRIMARY KEY (id),
    KEY k_type_updateDate (type, updateDate),
    KEY k_updateDate (updateDate),
    KEY k_updateUserId_updateDate (updateUserId, updateDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS RecordHistory;
CREATE TABLE RecordHistory (
    id BIGINT(20) NOT NULL AUTO_INCREMENT,
    recordId BIGINT(20) NOT NULL,
    type VARCHAR(20) NOT NULL,
    data LONGTEXT NOT NULL,
    updateDate BIGINT(20) NOT NULL,
    updateUserId BIGINT(20) NOT NULL,
    PRIMARY KEY (id),
    KEY k_recordId_updateDate (recordId, updateDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS RecordInput;
CREATE TABLE RecordInput (
    recordId BIGINT(20) NOT NULL,
    data LONGTEXT NOT NULL,
    updateDate BIGINT(20) NOT NULL,
    PRIMARY KEY (recordId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS RecordLink;
CREATE TABLE RecordLink (
    parentId BIGINT(20) NOT NULL,
    childId BIGINT(20) NOT NULL,
    KEY k_parentId (parentId),
    KEY k_childId (childId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS RecordName;
CREATE TABLE RecordName (
    recordId BIGINT(20) NOT NULL,
    namespace VARCHAR(20) NOT NULL,
    name VARCHAR(200) NOT NULL,
    KEY k_recordId (recordId),
    KEY k_namespace_name (namespace, name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS RecordNumber;
CREATE TABLE RecordNumber (
    recordId BIGINT(20) NOT NULL,
    namespace VARCHAR(20) NOT NULL,
    number DOUBLE NOT NULL,
    KEY k_namespace_recordId (namespace, recordId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS RecordOutput;
CREATE TABLE RecordOutput (
    recordId BIGINT(20) NOT NULL,
    format VARCHAR(20) NOT NULL,
    data LONGTEXT NOT NULL,
    updateDate BIGINT(20) NOT NULL,
    PRIMARY KEY (recordId, format)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
