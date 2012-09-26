INSERT IGNORE INTO Symbol (value) SELECT DISTINCT name FROM RecordLocation;
INSERT IGNORE INTO Symbol (value) SELECT DISTINCT name FROM RecordNumber;
INSERT IGNORE INTO Symbol (value) SELECT DISTINCT name FROM RecordString;
INSERT IGNORE INTO Symbol (value) SELECT DISTINCT name FROM RecordUuid;

INSERT IGNORE INTO RecordLocation2 (id, symbolId, value)
SELECT i.recordId, s.symbolId, i.value
FROM RecordLocation AS i
INNER JOIN Symbol AS s ON i.name = s.value;

INSERT IGNORE INTO RecordNumber2 (id, symbolId, value)
SELECT i.recordId, s.symbolId, i.value FROM RecordNumber AS i
INNER JOIN Symbol AS s ON i.name = s.value;

INSERT IGNORE INTO RecordString2 (id, symbolId, value)
SELECT i.recordId, s.symbolId, i.value FROM RecordString AS i
INNER JOIN Symbol AS s ON i.name = s.value;

INSERT IGNORE INTO RecordUuid2 (id, symbolId, value)
SELECT i.recordId, s.symbolId, i.value FROM RecordUuid AS i
INNER JOIN Symbol AS s ON i.name = s.value;

/*
DROP TABLE RecordLocation;
DROP TABLE RecordNumber;
DROP TABLE RecordString;
DROP TABLE RecordUuid;
*/
