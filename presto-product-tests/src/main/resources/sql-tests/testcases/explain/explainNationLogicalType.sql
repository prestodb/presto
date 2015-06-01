-- database: presto; groups: explain,base_sql; queryType: SELECT; tables: nation
EXPLAIN SELECT
          n_regionkey,
          count(*)
        FROM nation
        GROUP BY n_regionkey
        ORDER BY n_regionkey