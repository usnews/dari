DROP TABLE IF EXISTS Record;
CREATE TABLE Record (
    id BIGINT(20) NOT NULL AUTO_INCREMENT,
    type VARCHAR(20) NOT NULL,
    name VARCHAR(200) NOT NULL,
    data LONGTEXT NOT NULL,
    createDate DATETIME NOT NULL,
    createUserId BIGINT(20) NOT NULL,
    updateDate DATETIME NOT NULL,
    updateUserId BIGINT(20) NOT NULL,
    deleteDate DATETIME NULL,
    deleteUserId BIGINT(20) NOT NULL,
    PRIMARY KEY (id),
    KEY k_type_name (type, name),
    KEY k_type_updateDate (type, updateDate),
    KEY k_updateDate (updateDate),
    KEY k_updateUserId_updateDate (updateUserId, updateDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS RecordHistory;
CREATE TABLE RecordHistory (
    id BIGINT(20) NOT NULL AUTO_INCREMENT,
    recordId BIGINT(20) NOT NULL,
    type VARCHAR(20) NOT NULL,
    name VARCHAR(200) NOT NULL,
    data LONGTEXT NOT NULL,
    updateDate DATETIME NOT NULL,
    updateUserId BIGINT(20) NOT NULL,
    PRIMARY KEY (id),
    KEY k_recordId_updateDate (recordId, updateDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS RecordInput;
CREATE TABLE RecordInput (
    recordId BIGINT(20) NOT NULL,
    data LONGTEXT NOT NULL,
    updateDate DATETIME NOT NULL,
    PRIMARY KEY (recordId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS RecordLink;
CREATE TABLE RecordLink (
    parentId BIGINT(20) NOT NULL,
    childId BIGINT(20) NOT NULL,
    KEY k_parentId (parentId),
    KEY k_childId (childId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

DROP TABLE IF EXISTS RecordOutput;
CREATE TABLE RecordOutput (
    recordId BIGINT(20) NOT NULL,
    format VARCHAR(20) NOT NULL,
    data LONGTEXT NOT NULL,
    updateDate DATETIME NOT NULL,
    PRIMARY KEY (recordId, format)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
