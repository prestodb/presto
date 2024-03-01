-- database: presto; groups: join; tables: nation, region, part
SELECT p_partkey,
       n_name,
       r_name
FROM   part
       INNER JOIN nation
               ON n_regionkey = p_partkey
       RIGHT JOIN region
               ON n_nationkey = r_regionkey

