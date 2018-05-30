-- database: presto; groups: join; tables: nation, region
select * from nation join region on nation.n_regionkey = region.r_regionkey

