INSERT IGNORE INTO RecordString3 (id, symbolId, value)
SELECT id, symbolId, CONVERT(LOWER(TRIM(CONVERT(value USING utf8))) USING binary) FROM RecordString2;

/*
DROP TABLE RecordString2;
*/
