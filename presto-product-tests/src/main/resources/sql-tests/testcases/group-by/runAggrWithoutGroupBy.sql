-- database: presto; requires: com.facebook.presto.tests.ImmutableTpchTablesRequirements; tables: workers; groups: group-by;
select count(*), count(n_regionkey), min(n_regionkey), max(n_regionkey), sum(n_regionkey) from nation
