-- database: presto; groups: with_clause; tables: nation; queryType: SELECT
WITH ordered AS (select n_nationkey a, n_regionkey b, n_name c from nation order by 1,2 limit 10)
select * from  ordered order by 1,2 limit 5
