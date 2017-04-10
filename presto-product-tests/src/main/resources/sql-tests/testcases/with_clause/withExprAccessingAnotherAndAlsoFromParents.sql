-- database: presto; groups: with_clause; tables: nation; queryType: SELECT
WITH w1 AS (select * from nation),
w2 AS (select * from w1)
select count(*) from w1, w2
