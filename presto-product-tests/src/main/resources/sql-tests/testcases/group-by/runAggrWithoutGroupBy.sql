-- database: presto; requires: com.teradata.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements; tables: workers; groups: group-by;
select count(*), count(n_regionkey), min(n_regionkey), max(n_regionkey), sum(n_regionkey) from nation
