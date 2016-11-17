-- database: presto; requires: com.teradata.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements; tables: workers; groups: group-by;
SELECT first_name, COUNT(*) FROM workers GROUP BY first_name HAVING first_name IS NULL
