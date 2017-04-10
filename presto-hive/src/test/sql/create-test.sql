CREATE TABLE presto_test_sequence (
  n INT
)
COMMENT 'Presto test data'
TBLPROPERTIES ('RETENTION'='-1')
;

CREATE TABLE presto_test_partition_format (
  t_string STRING,
  t_tinyint TINYINT,
  t_smallint SMALLINT,
  t_int INT,
  t_bigint BIGINT,
  t_float FLOAT,
  t_double DOUBLE,
  t_boolean BOOLEAN
)
COMMENT 'Presto test data'
PARTITIONED BY (ds STRING, file_format STRING, dummy INT)
TBLPROPERTIES ('RETENTION'='-1')
;

CREATE TABLE presto_test_unpartitioned (
  t_string STRING,
  t_tinyint TINYINT
)
COMMENT 'Presto test data'
STORED AS TEXTFILE
TBLPROPERTIES ('RETENTION'='-1')
;

CREATE TABLE presto_test_offline (
  t_string STRING
)
COMMENT 'Presto test data'
PARTITIONED BY (ds STRING)
TBLPROPERTIES ('RETENTION'='-1', 'PROTECT_MODE'='OFFLINE')
;

CREATE TABLE presto_test_offline_partition (
  t_string STRING
)
COMMENT 'Presto test data'
PARTITIONED BY (ds STRING)
TBLPROPERTIES ('RETENTION'='-1')
;

CREATE TABLE presto_test_bucketed_by_string_int (
  t_string STRING,
  t_tinyint TINYINT,
  t_smallint SMALLINT,
  t_int INT,
  t_bigint BIGINT,
  t_float FLOAT,
  t_double DOUBLE,
  t_boolean BOOLEAN
)
COMMENT 'Presto test bucketed table'
PARTITIONED BY (ds STRING)
CLUSTERED BY (t_string, t_int) INTO 32 BUCKETS
STORED AS RCFILE
TBLPROPERTIES ('RETENTION'='-1')
;

CREATE TABLE presto_test_bucketed_by_bigint_boolean (
  t_string STRING,
  t_tinyint TINYINT,
  t_smallint SMALLINT,
  t_int INT,
  t_bigint BIGINT,
  t_float FLOAT,
  t_double DOUBLE,
  t_boolean BOOLEAN
)
COMMENT 'Presto test bucketed table'
PARTITIONED BY (ds STRING)
CLUSTERED BY (t_bigint, t_boolean) INTO 32 BUCKETS
STORED AS RCFILE
TBLPROPERTIES ('RETENTION'='-1')
;

CREATE TABLE presto_test_bucketed_by_double_float (
  t_string STRING,
  t_tinyint TINYINT,
  t_smallint SMALLINT,
  t_int INT,
  t_bigint BIGINT,
  t_float FLOAT,
  t_double DOUBLE,
  t_boolean BOOLEAN
)
COMMENT 'Presto test bucketed table'
PARTITIONED BY (ds STRING)
CLUSTERED BY (t_double, t_float) INTO 32 BUCKETS
STORED AS RCFILE
TBLPROPERTIES ('RETENTION'='-1')
;

CREATE TABLE presto_test_partition_schema_change (
  t_data STRING,
  t_extra STRING
)
COMMENT 'Presto test partition schema change'
PARTITIONED BY (ds STRING)
STORED AS TEXTFILE
TBLPROPERTIES ('RETENTION'='-1')
;

CREATE TABLE presto_test_partition_schema_change_non_canonical (
  t_data STRING
)
COMMENT 'Presto test non-canonical boolean partition table'
PARTITIONED BY (t_boolean BOOLEAN)
TBLPROPERTIES ('RETENTION'='-1')
;

CREATE VIEW presto_test_view
COMMENT 'Presto test view'
TBLPROPERTIES ('RETENTION'='-1')
AS SELECT * FROM presto_test_unpartitioned
;

DROP TABLE IF EXISTS tmp_presto_test_load;
CREATE TABLE tmp_presto_test_load (word STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '/usr/share/dict/words'
INTO TABLE tmp_presto_test_load
;

INSERT OVERWRITE TABLE presto_test_sequence
SELECT TRANSFORM(word)
USING 'awk "BEGIN { n = 0 } { print ++n }"' AS n
FROM tmp_presto_test_load
LIMIT 100
;

DROP TABLE tmp_presto_test_load;

DROP TABLE IF EXISTS tmp_presto_test;
CREATE TABLE tmp_presto_test (
  t_string STRING,
  t_tinyint TINYINT,
  t_smallint SMALLINT,
  t_int INT,
  t_bigint BIGINT,
  t_float FLOAT,
  t_double DOUBLE,
  t_boolean BOOLEAN
)
;
INSERT INTO TABLE tmp_presto_test
SELECT
  CASE n % 19 WHEN 0 THEN NULL WHEN 1 THEN '' ELSE 'test' END
, 1 + n
, 2 + n
, 3 + n
, 4 + n + CASE WHEN n % 13 = 0 THEN NULL ELSE 0 END
, 5.1 + n
, 6.2 + n
, CASE n % 3 WHEN 0 THEN false WHEN 1 THEN true ELSE NULL END
FROM presto_test_sequence
LIMIT 100
;

ALTER TABLE presto_test_partition_format SET FILEFORMAT TEXTFILE;
ALTER TABLE presto_test_partition_format SET SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe';
ALTER TABLE presto_test_partition_format ADD PARTITION (ds='2012-12-29', file_format='textfile', dummy=1);
INSERT INTO TABLE presto_test_partition_format PARTITION (ds='2012-12-29', file_format='textfile', dummy=1)
SELECT * FROM tmp_presto_test
;

ALTER TABLE presto_test_partition_format SET FILEFORMAT SEQUENCEFILE;
ALTER TABLE presto_test_partition_format SET SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe';
ALTER TABLE presto_test_partition_format ADD PARTITION (ds='2012-12-29', file_format='sequencefile', dummy=2);
INSERT INTO TABLE presto_test_partition_format PARTITION (ds='2012-12-29', file_format='sequencefile', dummy=2)
SELECT * FROM tmp_presto_test
;

ALTER TABLE presto_test_partition_format SET FILEFORMAT RCFILE;
ALTER TABLE presto_test_partition_format SET SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe';
ALTER TABLE presto_test_partition_format ADD PARTITION (ds='2012-12-29', file_format='rctext', dummy=3);
INSERT INTO TABLE presto_test_partition_format PARTITION (ds='2012-12-29', file_format='rctext', dummy=3)
SELECT * FROM tmp_presto_test
;

ALTER TABLE presto_test_partition_format SET FILEFORMAT RCFILE;
ALTER TABLE presto_test_partition_format SET SERDE 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe';
ALTER TABLE presto_test_partition_format ADD PARTITION (ds='2012-12-29', file_format='rcbinary', dummy=4);
INSERT INTO TABLE presto_test_partition_format PARTITION (ds='2012-12-29', file_format='rcbinary', dummy=4)
SELECT * FROM tmp_presto_test
;

INSERT INTO TABLE presto_test_unpartitioned
SELECT
  CASE n % 19 WHEN 0 THEN NULL WHEN 1 THEN '' ELSE 'unpartitioned' END
, 1 + n
FROM presto_test_sequence LIMIT 100;

INSERT INTO TABLE presto_test_offline_partition PARTITION (ds='2012-12-29')
SELECT 'test' FROM presto_test_sequence LIMIT 100;

INSERT INTO TABLE presto_test_offline_partition PARTITION (ds='2012-12-30')
SELECT 'test' FROM presto_test_sequence LIMIT 100;

ALTER TABLE presto_test_offline_partition PARTITION (ds='2012-12-30') ENABLE OFFLINE;

SET hive.enforce.bucketing = true;

INSERT OVERWRITE TABLE presto_test_bucketed_by_string_int
PARTITION (ds='2012-12-29')
SELECT t_string, t_tinyint, t_smallint, t_int, t_bigint, t_float, t_double, t_boolean
FROM tmp_presto_test
;

INSERT OVERWRITE TABLE presto_test_bucketed_by_bigint_boolean
PARTITION (ds='2012-12-29')
SELECT t_string, t_tinyint, t_smallint, t_int, t_bigint, t_float, t_double, t_boolean
FROM tmp_presto_test
;

INSERT OVERWRITE TABLE presto_test_bucketed_by_double_float
PARTITION (ds='2012-12-29')
SELECT t_string, t_tinyint, t_smallint, t_int, t_bigint, t_float, t_double, t_boolean
FROM tmp_presto_test
;

DROP TABLE tmp_presto_test;

ALTER TABLE presto_test_partition_schema_change ADD PARTITION (ds='2012-12-29');
INSERT OVERWRITE TABLE presto_test_partition_schema_change PARTITION (ds='2012-12-29')
SELECT '123', '456' FROM presto_test_sequence;
ALTER TABLE presto_test_partition_schema_change REPLACE COLUMNS (t_data DOUBLE);

INSERT OVERWRITE TABLE presto_test_partition_schema_change_non_canonical PARTITION (t_boolean='0')
SELECT 'test' FROM presto_test_sequence LIMIT 100;
