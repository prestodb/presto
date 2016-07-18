-- database: presto; requires: com.facebook.presto.tests.ImmutableTpchTablesRequirements; tables: workers; groups: group-by;
select n_regionkey, count(*), sum(n_regionkey) from nation where n_regionkey > 2 group by 1
