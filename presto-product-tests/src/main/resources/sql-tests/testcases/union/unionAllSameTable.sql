-- database: presto; requires: com.teradata.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements; tables: nation; groups: union;
SELECT *
FROM nation
UNION ALL
SELECT *
FROM nation
