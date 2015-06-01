-- database: presto; requires: com.facebook.presto.tests.ImmutableTpchTablesRequirements; tables: workers; groups: group-by;
SELECT COUNT(*) FROM nation HAVING COUNT(*) > 20
