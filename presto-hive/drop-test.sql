DROP TABLE IF EXISTS presto_test;

DROP TABLE IF EXISTS presto_test_unpartitioned;

CREATE TABLE IF NOT EXISTS presto_test_offline (x INT);
ALTER TABLE presto_test_offline DISABLE OFFLINE;
DROP TABLE IF EXISTS presto_test_offline;

CREATE TABLE IF NOT EXISTS presto_test_offline_partition (x INT) PARTITIONED BY (ds STRING);
ALTER TABLE presto_test_offline_partition ADD IF NOT EXISTS PARTITION (ds='2012-12-30');
ALTER TABLE presto_test_offline_partition PARTITION (ds='2012-12-30') DISABLE OFFLINE;
DROP TABLE IF EXISTS presto_test_offline_partition;

DROP TABLE IF EXISTS presto_test_bucketed_by_string_int;
DROP TABLE IF EXISTS presto_test_bucketed_by_bigint_boolean;
DROP TABLE IF EXISTS presto_test_bucketed_by_double_float;

DROP VIEW IF EXISTS presto_test_view;
