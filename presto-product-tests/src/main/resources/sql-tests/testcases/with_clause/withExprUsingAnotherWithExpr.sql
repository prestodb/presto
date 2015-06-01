-- database: presto; groups: with_clause; tables: nation; queryType: SELECT
WITH w1 AS (select min(n_nationkey) as x , max(n_regionkey) as y from nation),
w2 AS (select x, y from w1)
select count(*) count, n_regionkey from nation group by n_regionkey
union all
select * from w2 order by n_regionkey, count
