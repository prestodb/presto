-- database: presto; tables: nation; groups: group-by;
select count(*), count(n_regionkey), min(n_regionkey), max(n_regionkey), sum(n_regionkey) from nation
