-- database: presto; groups: join; tables: nation, region
select n_name, r_name from region right outer join nation on n_nationkey = r_regionkey

