-- database: presto; groups: explain,quarantine; queryType: SELECT; tables: nation
EXPLAIN (FORMAT GRAPHVIZ) SELECT
          n_regionkey,
          count(*)
        FROM nation
        GROUP BY n_regionkey
        ORDER BY n_regionkey