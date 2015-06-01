-- database: presto; groups: explain; queryType: SELECT; tables: nation
EXPLAIN (TYPE DISTRIBUTED) SELECT
                             n_regionkey,
                             count(*)
                           FROM nation
                           GROUP BY n_regionkey
                           ORDER BY n_regionkey