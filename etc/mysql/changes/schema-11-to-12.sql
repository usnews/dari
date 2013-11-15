INSERT IGNORE INTO RecordString4 (id, typeId, symbolId, value)
SELECT RecordString3.id, Record.typeId, RecordString3.symbolId, RecordString3.value FROM RecordString3 JOIN Record ON RecordString3.id = Record.id;

INSERT IGNORE INTO RecordLocation3 (id, typeId, symbolId, value)
SELECT RecordLocation2.id, Record.typeId, RecordLocation2.symbolId, RecordLocation2.value FROM RecordLocation2 JOIN Record ON RecordLocation2.id = Record.id;

INSERT IGNORE INTO RecordRegion2 (id, typeId, symbolId, value)
SELECT RecordRegion.id, Record.typeId, RecordRegion.symbolId, RecordRegion.value FROM RecordRegion JOIN Record ON RecordRegion.id = Record.id;

INSERT IGNORE INTO RecordNumber3 (id, typeId, symbolId, value)
SELECT RecordNumber2.id, Record.typeId, RecordNumber2.symbolId, RecordNumber2.value FROM RecordNumber2 JOIN Record ON RecordNumber2.id = Record.id;

INSERT IGNORE INTO RecordUuid3 (id, typeId, symbolId, value)
SELECT RecordUuid2.id, Record.typeId, RecordUuid2.symbolId, RecordUuid2.value FROM RecordUuid2 JOIN Record ON RecordUuid2.id = Record.id;

/*

-- DROP OLD TABLES:

DROP TABLE RecordString3;
DROP TABLE RecordLocation2;
DROP TABLE RecordRegion;
DROP TABLE RecordNumber2;
DROP TABLE RecordUuid2;

DROP VIEW RecordString3_d;
DROP VIEW RecordLocation2_d;
DROP VIEW RecordRegion_d;
DROP VIEW RecordNumber2_d;
DROP VIEW RecordUuid2_d;

-- OR --

-- RENAME OLD TABLES:
ALTER TABLE RecordString3 RENAME TO RecordString3_bak;
ALTER TABLE RecordLocation2 RENAME TO RecordLocation2_bak;
ALTER TABLE RecordRegion RENAME TO RecordRegion_bak;
ALTER TABLE RecordNumber2 RENAME TO RecordNumber2_bak;
ALTER TABLE RecordUuid2 RENAME TO RecordUuid2_bak;

-- REVERT BACK TO OLD TABLES:
ALTER TABLE RecordString3_bak RENAME TO RecordString3;
ALTER TABLE RecordLocation2_bak RENAME TO RecordLocation2;
ALTER TABLE RecordRegion_bak RENAME TO RecordRegion;
ALTER TABLE RecordNumber2_bak RENAME TO RecordNumber2;
ALTER TABLE RecordUuid2_bak RENAME TO RecordUuid2;

-- DROP NEW TABLES:
DROP TABLE RecordString4;
DROP TABLE RecordLocation3;
DROP TABLE RecordRegion2;
DROP TABLE RecordNumber3;
DROP TABLE RecordUuid3;

DROP VIEW RecordString4_d;
DROP VIEW RecordLocation3_d;
DROP VIEW RecordRegion2_d;
DROP VIEW RecordNumber3_d;
DROP VIEW RecordUuid3_d;

*/
