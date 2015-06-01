-- database: presto; groups: with_clause; tables: nation; queryType: SELECT
WITH ct AS (SELECT * FROM region) SELECT n_name FROM nation where n_nationkey = 0
