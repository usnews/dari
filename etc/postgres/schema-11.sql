CREATE TABLE IF NOT EXISTS "Record" (
    "id" UUID NOT NULL,
    "typeId" UUID NOT NULL,
    "data" bytea NOT NULL,
    PRIMARY KEY ("typeId", "id")
);
CREATE INDEX k_record_id ON Record ("id");
CREATE VIEW "Record_d" AS SELECT "id", "typeId", CONVERT_FROM(data, 'UTF-8') AS data FROM "Record";

CREATE EXTENSION postgis;

CREATE TABLE IF NOT EXISTS "RecordLocation2" (
    "id" UUID NOT NULL,
    "symbolId" INT NOT NULL,
    "value" GEOMETRY(POINT,4326) NOT NULL,
    PRIMARY KEY ("symbolId", "value", "id")
);
CREATE INDEX k_recordlocation_id ON "RecordLocation2" ("id");
CREATE INDEX k_recordlocation_value_gix ON "RecordLocation2" USING GIST ( "value" );

CREATE TABLE IF NOT EXISTS "RecordNumber2" (
    "id" UUID NOT NULL,
    "symbolId" INT NOT NULL,
    "value" DECIMAL NOT NULL,
    PRIMARY KEY ("symbolId", "value", "id")
);
CREATE INDEX k_recordnumber2_id ON "RecordNumber2" ("id");

CREATE TABLE IF NOT EXISTS "RecordString3" (
    "id" UUID NOT NULL,
    "symbolId" INT NOT NULL,
    "value" bytea NOT NULL,
    PRIMARY KEY ("symbolId", "value", "id")
);
CREATE INDEX k_recordstring3_id ON "RecordString3" ("id");
CREATE VIEW "RecordString3_d" AS SELECT "id", "symbolId", CONVERT_FROM("value", 'UTF-8') AS "value" FROM "RecordString3";

CREATE TABLE IF NOT EXISTS "RecordUpdate" (
    "id" UUID NOT NULL,
    "typeId" UUID NOT NULL,
    "updateDate" DECIMAL NOT NULL,
    PRIMARY KEY ("id")
);
CREATE INDEX k_recordstring3_typeId_updateDate ON "RecordUpdate" ("typeId", "updateDate");
CREATE INDEX k_recordstring3_updateDate ON "RecordUpdate" ("updateDate");

CREATE TABLE IF NOT EXISTS "RecordUuid2" (
    "id" UUID NOT NULL,
    "symbolId" INT NOT NULL,
    "value" UUID NOT NULL,
    PRIMARY KEY ("symbolId", "value", "id")
);
CREATE INDEX k_recorduuid2_id ON "RecordUuid2" ("id");

CREATE SEQUENCE "symbol_seq";
CREATE TABLE IF NOT EXISTS "Symbol" (
    "symbolId" INT NOT NULL DEFAULT NEXTVAL('symbol_seq'),
    "value" bytea NOT NULL,
    PRIMARY KEY ("symbolId")
);
CREATE UNIQUE INDEX k_symbol_value ON "Symbol" ("value");
CREATE VIEW "Symbol_d" AS SELECT "symbolId", CONVERT_FROM("value", 'UTF-8') AS "value" FROM "Symbol";
