-- database: presto; groups: join; tables: nation, region
select n_name, r_name from nation cross join region

