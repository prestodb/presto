-- database: presto; groups: join; tables: nation, region
SELECT n_name
FROM   nation
       LEFT JOIN region
              ON n_nationkey = r_regionkey
WHERE  r_name is null

