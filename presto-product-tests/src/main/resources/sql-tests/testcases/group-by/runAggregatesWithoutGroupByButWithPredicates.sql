-- database: presto; requires: com.facebook.presto.tests.ImmutableTpchTablesRequirements; tables: workers; groups: group-by;
select count(*), sum(n_nationkey) from nation where 1=2
