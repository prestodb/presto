-- database: presto; groups: join; tables: nation, part
SELECT p_partkey,
       n_name
FROM   nation
       INNER JOIN part
              ON n_nationkey = p_partkey
WHERE  n_name < p_name

