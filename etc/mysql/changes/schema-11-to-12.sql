INSERT IGNORE INTO RecordString4 (id, typeId, symbolId, value)
SELECT RecordString3.id, Record.typeId, RecordString3.symbolId, RecordString3.value FROM RecordString3 JOIN Record ON RecordString3.id = Record.id;

INSERT IGNORE INTO RecordLocation3 (id, typeId, symbolId, value)
SELECT RecordLocation2.id, Record.typeId, RecordLocation2.symbolId, RecordLocation2.value FROM RecordLocation2 JOIN Record ON RecordLocation2.id = Record.id;

INSERT IGNORE INTO RecordNumber3 (id, typeId, symbolId, value)
SELECT RecordNumber2.id, Record.typeId, RecordNumber2.symbolId, RecordNumber2.value FROM RecordNumber2 JOIN Record ON RecordNumber2.id = Record.id;

INSERT IGNORE INTO RecordUuid3 (id, typeId, symbolId, value)
SELECT RecordUuid2.id, Record.typeId, RecordUuid2.symbolId, RecordUuid2.value FROM RecordUuid2 JOIN Record ON RecordUuid2.id = Record.id;

/*
DROP TABLE RecordString2;
*/

