-- database: presto; groups: with_clause; tables: nation; queryType: SELECT
WITH nested AS (SELECT * FROM nation) SELECT count(*) FROM (select * FROM nested) as a
