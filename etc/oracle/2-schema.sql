
CREATE TABLE Record (
    id RAW(16) NOT NULL,
    typeId RAW(16) NOT NULL,
    data BLOB NOT NULL,
    CONSTRAINT pk_r_typeid_id PRIMARY KEY (typeId, id)
) ORGANIZATION INDEX COMPRESS 1
  TABLESPACE dari;
CREATE INDEX k_r_id ON Record (id);

CREATE TABLE RecordLocation2 (
    id RAW(16) NOT NULL,
    symbolId NUMBER(5) NOT NULL,
    value SDO_GEOMETRY NOT NULL,
    CONSTRAINT pk_rl_symbolid_id PRIMARY KEY (symbolId, ID)
) TABLESPACE dari;
CREATE INDEX k_rl_id ON RecordLocation2 (ID);
INSERT INTO USER_SDO_GEOM_METADATA (TABLE_NAME, COLUMN_NAME, DIMINFO, SRID) VALUES ('RECORDLOCATION2', 'VALUE', MDSYS.SDO_DIM_ARRAY (MDSYS.SDO_DIM_ELEMENT('LONG', -180.0, 180.0, 0.005), MDSYS.SDO_DIM_ELEMENT('LAT', -90.0, 90.0, 0.005)), 8307);
CREATE INDEX k_rl2_value ON RecordLocation2(VALUE) indextype is mdsys.spatial_index;

CREATE TABLE RecordNumber2 (
    id RAW(16) NOT NULL,
    symbolId NUMBER(5) NOT NULL,
    value NUMBER(*,3) NOT NULL,
    CONSTRAINT pk_rn2_symbolid_value_id PRIMARY KEY (symbolId, value, id)
) ORGANIZATION INDEX COMPRESS
  TABLESPACE dari;
CREATE INDEX k_rn2_id ON RecordNumber2 (id);

CREATE TABLE RecordString3 (
    id RAW(16) NOT NULL,
    symbolId NUMBER(5) NOT NULL,
    value RAW(500) NOT NULL,
    CONSTRAINT pk_rs3_symbolid_value_id PRIMARY KEY (symbolId, value, id)
) ORGANIZATION INDEX COMPRESS
  TABLESPACE dari;
CREATE INDEX k_rs3_id ON RecordString3 (id);

CREATE TABLE RecordUpdate (
    id RAW(16) NOT NULL,
    typeId RAW(16) NOT NULL,
    updateDate DECIMAL NOT NULL,
    PRIMARY KEY (id)
) TABLESPACE dari;
CREATE INDEX k_rs3_typeId_updateDate ON RecordUpdate (typeId, updateDate);
CREATE INDEX k_rs3_updateDate ON RecordUpdate (updateDate);

CREATE TABLE RecordUuid2 (
    id RAW(16) NOT NULL,
    symbolId NUMBER(5) NOT NULL,
    value RAW(16) NOT NULL,
    CONSTRAINT pk_ru2_symbolid_value_id PRIMARY KEY (symbolId, value, id)
) ORGANIZATION INDEX COMPRESS
  TABLESPACE dari;
CREATE INDEX k_ru2_id ON RecordUuid2 (id);

CREATE SEQUENCE symbol_seq;

CREATE TABLE Symbol (
    symbolId NUMBER(5) NOT NULL,
    value RAW(500) NOT NULL,
    CONSTRAINT pk_s_symbolid PRIMARY KEY (symbolId)
) TABLESPACE dari;
CREATE UNIQUE INDEX k_s_value ON Symbol (value);

CREATE OR REPLACE TRIGGER symbol_seq BEFORE INSERT ON Symbol
FOR EACH ROW
BEGIN
    SELECT symbol_seq.NEXTVAL into :new.symbolId FROM dual;
END;

