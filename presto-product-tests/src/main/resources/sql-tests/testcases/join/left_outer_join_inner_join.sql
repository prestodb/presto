-- database: presto; groups: join; tables: nation, region, part
SELECT p_partkey,
       n_name,
       r_name
FROM   nation
       LEFT JOIN region
              ON n_nationkey = r_regionkey
       INNER JOIN part
               ON n_regionkey = p_partkey

