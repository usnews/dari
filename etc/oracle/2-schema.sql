
CREATE TABLE Record (
    id RAW(16) NOT NULL,
    typeId RAW(16) NOT NULL,
    data BLOB NOT NULL,
    CONSTRAINT pk_r_typeid_id PRIMARY KEY (typeId, id)
) ORGANIZATION INDEX COMPRESS 1
  TABLESPACE dari;
CREATE INDEX k_r_id ON Record (id);

CREATE TABLE RecordLocation3 (
    id RAW(16) NOT NULL,
    typeId RAW(16) NOT NULL,
    symbolId NUMBER(5) NOT NULL,
    value SDO_GEOMETRY NOT NULL,
    CONSTRAINT pk_rl3_symboltypeid_id PRIMARY KEY (symbolId, typeId, id)
) TABLESPACE dari;
CREATE INDEX k_rl3_id ON RecordLocation3 (id);
INSERT INTO USER_SDO_GEOM_METADATA (TABLE_NAME, COLUMN_NAME, DIMINFO, SRID) VALUES ('RECORDLOCATION3', 'VALUE', MDSYS.SDO_DIM_ARRAY (MDSYS.SDO_DIM_ELEMENT('LONG', -180.0, 180.0, 0.005), MDSYS.SDO_DIM_ELEMENT('LAT', -90.0, 90.0, 0.005)), 8307);
CREATE INDEX k_rl3_value ON RecordLocation3(VALUE) indextype is mdsys.spatial_index;

CREATE TABLE RecordNumber3 (
    id RAW(16) NOT NULL,
    typeId RAW(16) NOT NULL,
    symbolId NUMBER(5) NOT NULL,
    value NUMBER(*,3) NOT NULL,
    CONSTRAINT pk_rn3_symbolid_value_type_id PRIMARY KEY (symbolId, value, typeId, id)
) ORGANIZATION INDEX COMPRESS
  TABLESPACE dari;
CREATE INDEX k_rn3_id ON RecordNumber2 (id);

CREATE TABLE RecordString4 (
    id RAW(16) NOT NULL,
    typeId RAW(16) NOT NULL,
    symbolId NUMBER(5) NOT NULL,
    value RAW(500) NOT NULL,
    CONSTRAINT pk_rs4_symbolid_value_type_id PRIMARY KEY (symbolId, value, typeId, id)
) ORGANIZATION INDEX COMPRESS
  TABLESPACE dari;
CREATE INDEX k_rs4_id ON RecordString4 (id);

CREATE TABLE RecordUpdate (
    id RAW(16) NOT NULL,
    typeId RAW(16) NOT NULL,
    updateDate DECIMAL NOT NULL,
    PRIMARY KEY (id)
) TABLESPACE dari;
CREATE INDEX k_r3_typeId_updateDate ON RecordUpdate (typeId, updateDate);
CREATE INDEX k_ru_updateDate ON RecordUpdate (updateDate);

CREATE TABLE RecordUuid3 (
    id RAW(16) NOT NULL,
    typeId RAW(16) NOT NULL,
    symbolId NUMBER(5) NOT NULL,
    value RAW(16) NOT NULL,
    CONSTRAINT pk_ru3_symbolid_value_id PRIMARY KEY (symbolId, value, typeId, id)
) ORGANIZATION INDEX COMPRESS
  TABLESPACE dari;
CREATE INDEX k_ru3_id ON RecordUuid3 (id);

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

