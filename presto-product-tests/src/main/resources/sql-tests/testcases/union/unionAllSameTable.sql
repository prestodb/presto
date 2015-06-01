-- database: presto; requires: com.facebook.presto.tests.ImmutableTpchTablesRequirements; tables: nation; groups: union;
SELECT *
FROM nation
UNION ALL
SELECT *
FROM nation
