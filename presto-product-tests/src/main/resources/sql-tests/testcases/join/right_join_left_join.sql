-- database: presto; groups: join; tables: nation, region, part
SELECT p_partkey,
       n_name,
       r_name
FROM   part
       RIGHT OUTER JOIN nation
                     ON n_regionkey = p_partkey
       LEFT OUTER JOIN region
                    ON n_nationkey = r_regionkey

