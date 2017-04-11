-- database: presto; tables: nation; groups: group-by;
select n_regionkey, count(*), sum(n_nationkey) from nation group by 1
