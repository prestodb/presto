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

CREATE TABLE presto_test_not_readable (
  t_string STRING
)
COMMENT 'Presto test data'
PARTITIONED BY (ds STRING)
TBLPROPERTIES ('RETENTION'='-1', 'object_not_readable'='reason for not readable')
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

ANALYZE TABLE presto_test_unpartitioned COMPUTE STATISTICS;
ANALYZE TABLE presto_test_unpartitioned COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE presto_test_bucketed_by_string_int PARTITION(ds) COMPUTE STATISTICS;
ANALYZE TABLE presto_test_bucketed_by_string_int PARTITION(ds) COMPUTE STATISTICS FOR COLUMNS;

CREATE TABLE presto_test_types_orc (
      t_string STRING
    , t_tinyint TINYINT
    , t_smallint SMALLINT
    , t_int INT
    , t_bigint BIGINT
    , t_float FLOAT
    , t_double DOUBLE
    , t_boolean BOOLEAN
    , t_timestamp TIMESTAMP
    , t_binary BINARY
    , t_date DATE
    , t_varchar VARCHAR(50)
    , t_char CHAR(25)
    , t_map MAP<STRING, STRING>
    , t_array_string ARRAY<STRING>
    , t_array_struct ARRAY<STRUCT<s_string: STRING, s_double:DOUBLE>>
    , t_struct STRUCT<s_string: STRING, s_double:DOUBLE>
    , t_complex MAP<INT, ARRAY<STRUCT<s_string: STRING, s_double:DOUBLE>>>
)
    STORED AS ORC
;

INSERT INTO TABLE presto_test_types_orc
SELECT
    CASE n % 19 WHEN 0 THEN NULL WHEN 1 THEN '' ELSE 'test' END
, 1 + n
, 2 + n
, 3 + n
, 4 + n + CASE WHEN n % 13 = 0 THEN NULL ELSE 0 END
, 5.1 + n
, 6.2 + n
, CASE n % 3 WHEN 0 THEN false WHEN 1 THEN true ELSE NULL END
, CASE WHEN n % 17 = 0 THEN NULL ELSE '2011-05-06 07:08:09.1234567' END
, CASE WHEN n % 23 = 0 THEN NULL ELSE CAST('test binary' AS BINARY) END
, CASE WHEN n % 37 = 0 THEN NULL ELSE '2013-08-09' END
, CASE n % 39 WHEN 0 THEN NULL WHEN 1 THEN '' ELSE 'test varchar' END
, CASE n % 41 WHEN 0 THEN NULL WHEN 1 THEN '' ELSE 'test char' END
, CASE WHEN n % 27 = 0 THEN NULL ELSE map('test key', 'test value') END
, CASE WHEN n % 29 = 0 THEN NULL ELSE array('abc', 'xyz', 'data') END
, CASE WHEN n % 31 = 0 THEN NULL ELSE
     array(named_struct('s_string', 'test abc', 's_double', 1e-1),
           named_struct('s_string' , 'test xyz', 's_double', 2e-1)) END
, CASE WHEN n % 31 = 0 THEN NULL ELSE
     named_struct('s_string', 'test abc', 's_double', 1e-1) END
, CASE WHEN n % 33 = 0 THEN NULL ELSE
     map(1, array(named_struct('s_string', 'test abc', 's_double', 1e-1),
                  named_struct('s_string' , 'test xyz', 's_double', 2e-1))) END
FROM presto_test_sequence
LIMIT 100
;


CREATE TABLE presto_test_types_sequencefile
STORED AS SEQUENCEFILE
AS
SELECT * FROM presto_test_types_orc;


CREATE TABLE presto_test_types_rctext
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
STORED AS RCFILE
AS
SELECT * FROM presto_test_types_orc;


CREATE TABLE presto_test_types_rcbinary
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe'
STORED AS RCFILE
AS
SELECT * FROM presto_test_types_orc;


CREATE TABLE presto_test_types_textfile
STORED AS TEXTFILE
AS
SELECT * FROM presto_test_types_orc;

-- Parquet is missing TIMESTAMP and BINARY.
CREATE TABLE presto_test_types_parquet (
                                           t_string STRING
    , t_varchar VARCHAR(50)
    , t_tinyint TINYINT
    , t_smallint SMALLINT
    , t_int INT
    , t_bigint BIGINT
    , t_float FLOAT
    , t_double DOUBLE
    , t_boolean BOOLEAN
    , t_timestamp TIMESTAMP
    , t_binary BINARY
    , t_map MAP<STRING, STRING>
    , t_array_string ARRAY<STRING>
    , t_array_struct ARRAY<STRUCT<s_string: STRING, s_double:DOUBLE>>
    , t_struct STRUCT<s_string: STRING, s_double:DOUBLE>
    , t_complex MAP<INT, ARRAY<STRUCT<s_string: STRING, s_double:DOUBLE>>>
)
    STORED AS PARQUET
;

INSERT INTO TABLE presto_test_types_parquet
SELECT
    t_string
     , t_varchar
     , t_tinyint
     , t_smallint
     , t_int
     , t_bigint
     , t_float
     , t_double
     , t_boolean
     , t_timestamp
     , t_binary
     , t_map
     , t_array_string
     , t_array_struct
     , t_struct
     , t_complex
FROM presto_test_types_orc
;

ALTER TABLE presto_test_types_textfile ADD COLUMNS (new_column INT);
ALTER TABLE presto_test_types_sequencefile ADD COLUMNS (new_column INT);
ALTER TABLE presto_test_types_rctext ADD COLUMNS (new_column INT);
ALTER TABLE presto_test_types_rcbinary ADD COLUMNS (new_column INT);
ALTER TABLE presto_test_types_orc ADD COLUMNS (new_column INT);
ALTER TABLE presto_test_types_parquet ADD COLUMNS (new_column INT);
