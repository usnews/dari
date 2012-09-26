DROP TABLE IF EXISTS RecordNumber;
CREATE TABLE RecordNumber (
    recordId BIGINT(20) NOT NULL,
    namespace VARCHAR(20) NOT NULL,
    number DOUBLE NOT NULL,
    KEY k_namespace_recordId (namespace, recordId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
