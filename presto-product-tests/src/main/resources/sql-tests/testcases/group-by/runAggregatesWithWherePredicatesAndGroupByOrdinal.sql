-- database: presto; tables: nation; groups: group-by;
select n_regionkey, count(*), sum(n_regionkey) from nation where n_regionkey > 2 group by 1
