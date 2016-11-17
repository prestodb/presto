-- database: presto; requires: com.teradata.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements; tables: workers; groups: group-by;
select n_regionkey, count(*), sum(n_nationkey) from nation group by 1
