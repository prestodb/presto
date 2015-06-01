-- database: presto; groups: join; tables: nation, region, part
SELECT p_partkey,
       n2.n_name,
       r_name
FROM   ( ( part
           RIGHT OUTER JOIN nation n1
                         ON n1.n_regionkey = p_partkey )
         LEFT OUTER JOIN region
                      ON n1.n_nationkey = r_regionkey )
       INNER JOIN nation n2
               ON n2.n_nationkey = r_regionkey
