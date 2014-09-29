CREATE EXTERNAL TABLE presto_test_s3 (
  t_bigint BIGINT,
  t_string STRING
)
COMMENT 'Presto test S3 table'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ('RETENTION'='-1')
;

ALTER TABLE presto_test_s3
SET LOCATION 's3://presto-test-hive/presto_test_s3'
;

DROP TABLE IF EXISTS presto_insert_destination_s3;
CREATE EXTERNAL TABLE presto_insert_destination_s3 (
 t_string STRING,
 t_int INT
)
COMMENT 'Presto Destination Table for Inserts'
STORED AS TEXTFILE
LOCATION 's3://presto-test-hive/presto_insert_destination'
;

DROP TABLE IF EXISTS presto_insert_destination_partitioned_s3;
CREATE EXTERNAL TABLE presto_insert_destination_partitioned_s3 (
 t_string STRING,
 t_int INT
)
COMMENT 'Presto Destination Table partitioned for Inserts'
PARTITIONED BY (ds STRING, dummy INT)
LOCATION 's3://presto-test-hive/presto_insert_destination_partitioned'
;

DROP TABLE IF EXISTS tmp_presto_test_load;
CREATE TABLE tmp_presto_test_load (word STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '/usr/share/dict/words'
INTO TABLE tmp_presto_test_load
;

DROP TABLE IF EXISTS tmp_presto_test;
CREATE TABLE tmp_presto_test (n INT);
INSERT OVERWRITE TABLE tmp_presto_test
SELECT TRANSFORM(word)
USING 'awk "BEGIN { n = 0 } { print ++n }"' AS n
FROM tmp_presto_test_load
LIMIT 100
;

INSERT INTO TABLE presto_insert_destination_s3 SELECT CONCAT("Val", n), n from tmp_presto_test limit 2;

INSERT INTO TABLE presto_insert_destination_partitioned_s3 partition(ds="2014-03-12", dummy=1) SELECT CONCAT("P1_Val", n), n from tmp_presto_test limit 2;
INSERT INTO TABLE presto_insert_destination_partitioned_s3 partition(ds="2014-03-12", dummy=2) SELECT CONCAT("P2_Val", n), 2 * n from tmp_presto_test limit 2;

DROP TABLE tmp_presto_test_load;
