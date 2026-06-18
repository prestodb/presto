-- database: presto; groups: join; tables: nation, region
select n_name from nation where n_nationkey in (select r_regionkey from region)

