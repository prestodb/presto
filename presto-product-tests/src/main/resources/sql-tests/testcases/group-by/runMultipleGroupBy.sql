-- database: presto; requires: com.facebook.presto.tests.ImmutableTpchTablesRequirements; tables: workers; groups: group-by;
SELECT COUNT(*), n_regionkey, n_nationkey FROM nation WHERE n_regionkey < 2 GROUP BY n_nationkey, n_regionkey ORDER BY n_regionkey, n_nationkey DESC
