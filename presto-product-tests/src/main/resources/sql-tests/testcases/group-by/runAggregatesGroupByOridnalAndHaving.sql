-- database: presto; tables: nation; groups: group-by;
select n_regionkey, count(*) from nation group by 1 having sum(n_regionkey) > 5 and sum(n_regionkey) < 20
