-- database: presto; tables: nation; groups: group-by;
select count(*), sum(n_nationkey) from nation where 1=2 group by n_regionkey
