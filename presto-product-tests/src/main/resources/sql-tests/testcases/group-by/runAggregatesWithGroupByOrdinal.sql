-- database: presto; requires: com.facebook.presto.tests.ImmutableTpchTablesRequirements; tables: workers; groups: group-by;
select n_regionkey, count(*), sum(n_nationkey) from nation group by 1
