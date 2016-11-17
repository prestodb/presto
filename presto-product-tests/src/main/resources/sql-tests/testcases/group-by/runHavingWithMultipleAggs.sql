-- database: presto; requires: com.teradata.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements; tables: workers; groups: group-by;
SELECT p_type, COUNT(*) FROM part GROUP BY p_type HAVING COUNT(*) > 20 and AVG(p_retailprice) > 1000
