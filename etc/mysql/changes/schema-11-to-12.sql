INSERT IGNORE INTO RecordString4 (typeId, id, symbolId, value)
SELECT Record.typeId, RecordString3.id, RecordString3.symbolId, RecordString3.value FROM RecordString3 JOIN Record ON RecordString3.id = Record.id;

INSERT IGNORE INTO RecordLocation3 (typeId, id, symbolId, value)
SELECT Record.typeId, RecordLocation2.id, RecordLocation2.symbolId, RecordLocation2.value FROM RecordLocation2 JOIN Record ON RecordLocation2.id = Record.id;

INSERT IGNORE INTO RecordNumber3 (typeId, id, symbolId, value)
SELECT Record.typeId, RecordNumber2.id, RecordNumber2.symbolId, RecordNumber2.value FROM RecordNumber2 JOIN Record ON RecordNumber2.id = Record.id;

INSERT IGNORE INTO RecordUuid3 (typeId, id, symbolId, value)
SELECT Record.typeId, RecordUuid2.id, RecordUuid2.symbolId, RecordUuid2.value FROM RecordUuid2 JOIN Record ON RecordUuid2.id = Record.id;

/*
DROP TABLE RecordString2;
*/

