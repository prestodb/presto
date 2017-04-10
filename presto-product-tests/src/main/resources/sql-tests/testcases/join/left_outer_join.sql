-- database: presto; groups: join; tables: nation, region
select n_name, r_name  from nation left outer join region on n_nationkey = r_regionkey

