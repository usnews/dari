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
DROP TABLE RecordString3;
DROP VIEW RecordString3_d;
DROP TABLE RecordLocation2;
DROP VIEW RecordLocation2_d;
DROP TABLE RecordRegion;
DROP VIEW RecordRegion_d;
DROP TABLE RecordNumber2;
DROP VIEW RecordNumber2_d;
DROP TABLE RecordUuid2;
DROP VIEW RecordUuid2_d;
*/
