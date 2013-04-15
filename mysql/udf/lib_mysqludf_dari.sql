DROP FUNCTION IF EXISTS dari_get_fields;
CREATE FUNCTION dari_get_fields RETURNS STRING SONAME 'lib_mysqludf_dari.so';

DROP FUNCTION IF EXISTS dari_get_value;
CREATE FUNCTION dari_get_value RETURNS STRING SONAME 'lib_mysqludf_dari.so';

DROP FUNCTION IF EXISTS dari_increment_metric;
CREATE FUNCTION dari_increment_metric RETURNS STRING SONAME 'lib_mysqludf_dari.so';
