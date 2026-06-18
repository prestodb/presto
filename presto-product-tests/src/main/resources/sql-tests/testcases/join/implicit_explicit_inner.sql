-- database: presto; groups: join; tables: nation, region, part
SELECT p_partkey,
       n_name,
       r_name
FROM   nation,
       region
       JOIN part
         ON r_regionkey = p_partkey
WHERE  n_nationkey = r_regionkey

