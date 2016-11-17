-- database: presto; requires: com.teradata.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements; tables: workers; groups: group-by;
SELECT COUNT(*) FROM workers GROUP BY salary * id_department HAVING salary * id_department IS NOT NULL
