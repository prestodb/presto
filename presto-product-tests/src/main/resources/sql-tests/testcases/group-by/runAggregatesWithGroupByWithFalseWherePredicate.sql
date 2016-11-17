-- database: presto; requires: com.teradata.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements; tables: workers; groups: group-by;
select count(*), sum(n_nationkey) from nation where 1=2 group by n_regionkey
