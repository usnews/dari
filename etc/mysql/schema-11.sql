
CREATE TABLE IF NOT EXISTS Record (
    id BINARY(16) NOT NULL,
    typeId BINARY(16) NOT NULL,
    data LONGBLOB NOT NULL,
    PRIMARY KEY (typeId, id),
    KEY k_id (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin ROW_FORMAT=DYNAMIC;
CREATE VIEW Record_d AS SELECT hex(id) AS id, hex(typeId) AS typeId, DATA FROM Record;

CREATE TABLE IF NOT EXISTS RecordLocation2 (
    id BINARY(16) NOT NULL,
    symbolId INT NOT NULL,
    value POINT NOT NULL,
    PRIMARY KEY (symbolId, value, id),
    KEY k_id (id),
    SPATIAL KEY k_value (value)
) ENGINE= MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_bin;
CREATE VIEW RecordLocation2_d AS SELECT hex(id) AS id, symbolId, VALUE FROM RecordLocation2;

CREATE TABLE `RecordRegion` (
  `id` binary(16) NOT NULL,
  `symbolId` int(11) NOT NULL,
  `value` polygon NOT NULL,
  PRIMARY KEY (`symbolId`,`value`(25),`id`),
  KEY `k_id` (`id`),
  SPATIAL KEY `k_value` (`value`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 COLLATE=latin1_bin;
CREATE VIEW RecordRegion_d AS SELECT hex(id) AS id, symbolId, VALUE FROM RecordRegion;

CREATE TABLE IF NOT EXISTS RecordNumber2 (
    id BINARY(16) NOT NULL,
    symbolId INT NOT NULL,
    value DOUBLE NOT NULL,
    PRIMARY KEY (symbolId, value, id),
    KEY k_id (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;
CREATE VIEW RecordNumber2_d AS SELECT hex(id) AS id, symbolId, VALUE FROM RecordNumber2;

CREATE TABLE IF NOT EXISTS RecordString3 (
    id BINARY(16) NOT NULL,
    symbolId INT NOT NULL,
    value VARBINARY(500) NOT NULL,
    PRIMARY KEY (symbolId, value, id),
    KEY k_id (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;
CREATE VIEW RecordString3_d AS SELECT hex(id) AS id, symbolId, VALUE FROM RecordString3;

CREATE TABLE IF NOT EXISTS RecordUpdate (
    id BINARY(16) NOT NULL,
    typeId BINARY(16) NOT NULL,
    updateDate DOUBLE NOT NULL,
    PRIMARY KEY (id),
    KEY k_typeId_updateDate (typeId, updateDate),
    KEY k_updateDate (updateDate)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;
CREATE VIEW RecordUpdate_d AS SELECT hex(id) AS id, hex(typeId) AS typeId, updateDate FROM RecordUpdate;

CREATE TABLE IF NOT EXISTS RecordUuid2 (
    id BINARY(16) NOT NULL,
    symbolId INT NOT NULL,
    value BINARY(16) NOT NULL,
    PRIMARY KEY (symbolId, value, id),
    KEY k_id (id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;
CREATE VIEW RecordUuid2_d AS SELECT hex(id) AS id, symbolId, hex(VALUE) AS VALUE FROM RecordUuid2;

CREATE TABLE IF NOT EXISTS Symbol (
    symbolId INT NOT NULL AUTO_INCREMENT,
    value VARBINARY(500) NOT NULL,
    PRIMARY KEY (symbolId),
    UNIQUE KEY k_value (value)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin;
