-- database: presto; groups: join; tables: nation, region
select n_name, r_name from nation join region on nation.n_regionkey = region.r_regionkey where n_name > 'E'

