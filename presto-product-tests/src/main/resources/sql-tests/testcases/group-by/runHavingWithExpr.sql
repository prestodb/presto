-- database: presto; requires: com.facebook.presto.tests.ImmutableTpchTablesRequirements; tables: workers; groups: group-by;
SELECT COUNT(*) FROM workers GROUP BY id_department * 2 HAVING SUM(log(10, salary + 1)) > 0
