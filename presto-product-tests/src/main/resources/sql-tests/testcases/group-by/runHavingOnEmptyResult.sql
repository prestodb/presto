-- database: presto; requires: com.teradata.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements; tables: workers; groups: group-by;
SELECT COUNT(n_regionkey) FROM nation WHERE 1=2 HAVING SUM(n_regionkey) IS NULL
