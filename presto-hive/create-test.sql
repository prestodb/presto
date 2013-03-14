CREATE TABLE presto_test (
  t_string STRING,
  t_tinyint TINYINT,
  t_smallint SMALLINT,
  t_int INT,
  t_bigint BIGINT,
  t_float FLOAT,
  t_double DOUBLE,
  t_map MAP<STRING, STRING>,
  t_boolean BOOLEAN,
  t_timestamp TIMESTAMP,
  t_binary BINARY,
  t_array_string ARRAY<STRING>,
  t_complex MAP<INT, ARRAY<STRUCT<s_string: STRING, s_double:DOUBLE>>>
)
COMMENT 'Presto test data'
PARTITIONED BY (ds STRING, file_format STRING, dummy INT)
TBLPROPERTIES('RETENTION'='-1')
;

CREATE TABLE presto_test_unpartitioned (
  t_string STRING,
  t_tinyint TINYINT
)
COMMENT 'Presto test data'
TBLPROPERTIES('RETENTION'='-1')
;

CREATE VIEW presto_test_view
COMMENT 'Presto test view'
TBLPROPERTIES('RETENTION'='-1')
AS SELECT * FROM presto_test_unpartitioned
;

DROP TABLE IF EXISTS tmp_presto_test;
CREATE TABLE tmp_presto_test AS
SELECT fb_number_rows() n FROM src LIMIT 100;

ALTER TABLE presto_test SET FILEFORMAT RCFILE;
ALTER TABLE presto_test SET SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe';
ALTER TABLE presto_test ADD PARTITION (ds='2012-12-29', file_format='rcfile', dummy=1);
INSERT INTO TABLE presto_test PARTITION (ds='2012-12-29', file_format='rcfile', dummy=1)
SELECT
  CASE WHEN n % 19 = 0 THEN NULL ELSE 'rcfile test' END
, 1 + n
, 2 + n
, 3 + n
, 4 + n + CASE WHEN n % 13 = 0 THEN NULL ELSE 0 END
, 5.1 + n
, 6.2 + n
, CASE WHEN n % 29 = 0 THEN NULL ELSE map('format', 'rcfile') END
, CASE n % 3 WHEN 0 THEN false WHEN 1 THEN true ELSE NULL END
, CASE WHEN n % 17 = 0 THEN NULL ELSE '2011-05-06 07:08:09.1234567' END
, CASE WHEN n % 23 = 0 THEN NULL ELSE CAST('rcfile test' AS BINARY) END
, CASE WHEN n % 27 = 0 THEN NULL ELSE array('rcfile', 'test', 'data') END
, CASE WHEN n % 31 = 0 THEN NULL ELSE map(1, array(named_struct('s_string', 'rcfile-a', 's_double', 0.1), named_struct('s_string' , 'rcfile-b', 's_double', 0.2))) END
FROM tmp_presto_test LIMIT 100;

ALTER TABLE presto_test SET FILEFORMAT SEQUENCEFILE;
ALTER TABLE presto_test SET SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe';
ALTER TABLE presto_test ADD PARTITION (ds='2012-12-29', file_format='sequencefile', dummy=2);
INSERT INTO TABLE presto_test PARTITION (ds='2012-12-29', file_format='sequencefile', dummy=2)
SELECT
  CASE WHEN n % 19 = 0 THEN NULL ELSE 'sequencefile test' END
, 201 + n
, 202 + n
, 203 + n
, 204 + n + CASE WHEN n % 13 = 0 THEN NULL ELSE 0 END
, 205.1 + n
, 206.2 + n
, CASE WHEN n % 29 = 0 THEN NULL ELSE map('format', 'sequencefile') END
, CASE n % 3 WHEN 0 THEN false WHEN 1 THEN true ELSE NULL END
, CASE WHEN n % 17 = 0 THEN NULL ELSE '2011-05-06 07:08:09.1234567' END
, CASE WHEN n % 23 = 0 THEN NULL ELSE CAST('sequencefile test' AS BINARY) END
, CASE WHEN n % 27 = 0 THEN NULL ELSE array('sequencefile', 'test', 'data') END
, CASE WHEN n % 31 = 0 THEN NULL ELSE map(1, array(named_struct('s_string', 'sequencefile-a', 's_double', 0.1), named_struct('s_string' , 'sequencefile-b', 's_double', 0.2))) END
FROM tmp_presto_test LIMIT 100;

ALTER TABLE presto_test SET FILEFORMAT TEXTFILE;
ALTER TABLE presto_test SET SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe';
ALTER TABLE presto_test ADD PARTITION (ds='2012-12-29', file_format='textfile', dummy=3);
INSERT INTO TABLE presto_test PARTITION (ds='2012-12-29', file_format='textfile', dummy=3)
SELECT
  CASE WHEN n % 19 = 0 THEN NULL ELSE 'textfile test' END
, 401 + n
, 402 + n
, 403 + n
, 404 + n + CASE WHEN n % 13 = 0 THEN NULL ELSE 0 END
, 405.1 + n
, 406.2 + n
, CASE WHEN n % 29 = 0 THEN NULL ELSE map('format', 'textfile') END
, CASE n % 3 WHEN 0 THEN false WHEN 1 THEN true ELSE NULL END
, CASE WHEN n % 17 = 0 THEN NULL ELSE '2011-05-06 07:08:09.1234567' END
, CASE WHEN n % 23 = 0 THEN NULL ELSE CAST('textfile test' AS BINARY) END
, CASE WHEN n % 27 = 0 THEN NULL ELSE array('textfile', 'test', 'data') END
, CASE WHEN n % 31 = 0 THEN NULL ELSE map(1, array(named_struct('s_string', 'textfile-a', 's_double', 0.1), named_struct('s_string' , 'textfile-b', 's_double', 0.2))) END
FROM tmp_presto_test LIMIT 100;

INSERT INTO TABLE presto_test_unpartitioned
SELECT
  CASE WHEN n % 19 = 0 THEN NULL ELSE 'unpartitioned' END
, 1 + n
FROM tmp_presto_test LIMIT 100;

DROP TABLE tmp_presto_test;
