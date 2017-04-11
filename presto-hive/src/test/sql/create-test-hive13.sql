CREATE TABLE presto_test_types_textfile (
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
, t_complex MAP<INT, ARRAY<STRUCT<s_string: STRING, s_double:DOUBLE>>>
)
STORED AS TEXTFILE
;

INSERT INTO TABLE presto_test_types_textfile
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
     array(named_struct('s_string', 'test abc', 's_double', 0.1),
           named_struct('s_string' , 'test xyz', 's_double', 0.2)) END
, CASE WHEN n % 33 = 0 THEN NULL ELSE
     map(1, array(named_struct('s_string', 'test abc', 's_double', 0.1),
                  named_struct('s_string' , 'test xyz', 's_double', 0.2))) END
FROM presto_test_sequence
LIMIT 100
;


CREATE TABLE presto_test_types_sequencefile LIKE presto_test_types_textfile;
ALTER TABLE presto_test_types_sequencefile SET FILEFORMAT SEQUENCEFILE;

INSERT INTO TABLE presto_test_types_sequencefile
SELECT * FROM presto_test_types_textfile
;


CREATE TABLE presto_test_types_rctext LIKE presto_test_types_textfile;
ALTER TABLE presto_test_types_rctext SET FILEFORMAT RCFILE;
ALTER TABLE presto_test_types_rctext SET SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe';

INSERT INTO TABLE presto_test_types_rctext
SELECT * FROM presto_test_types_textfile
;


CREATE TABLE presto_test_types_rcbinary LIKE presto_test_types_textfile;
ALTER TABLE presto_test_types_rcbinary SET FILEFORMAT RCFILE;
ALTER TABLE presto_test_types_rcbinary SET SERDE 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe';

INSERT INTO TABLE presto_test_types_rcbinary
SELECT * FROM presto_test_types_textfile
;


CREATE TABLE presto_test_types_orc LIKE presto_test_types_textfile;
ALTER TABLE presto_test_types_orc SET FILEFORMAT ORC;

INSERT INTO TABLE presto_test_types_orc
SELECT * FROM presto_test_types_textfile
;


-- Parquet fails when trying to use complex nested types.
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
, t_array_struct ARRAY<STRUCT<s_string:STRING, s_double:DOUBLE>>
, t_struct STRUCT<s_string:STRING, s_double:DOUBLE>
)
PARTITIONED BY (dummy INT)
STORED AS PARQUET
;

INSERT INTO TABLE presto_test_types_parquet
PARTITION (dummy=0)
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
, t_array_struct[0]
FROM presto_test_types_textfile
;

ALTER TABLE presto_test_types_parquet
CHANGE COLUMN t_struct t_struct STRUCT<s_string:STRING, s_double:DOUBLE, s_boolean:BOOLEAN>;

ALTER TABLE presto_test_types_textfile ADD COLUMNS (new_column INT);
ALTER TABLE presto_test_types_sequencefile ADD COLUMNS (new_column INT);
ALTER TABLE presto_test_types_rctext ADD COLUMNS (new_column INT);
ALTER TABLE presto_test_types_rcbinary ADD COLUMNS (new_column INT);
ALTER TABLE presto_test_types_orc ADD COLUMNS (new_column INT);
ALTER TABLE presto_test_types_parquet ADD COLUMNS (new_column INT);
