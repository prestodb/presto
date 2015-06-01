-- database: presto; requires: com.facebook.presto.tests.ImmutableTpchTablesRequirements; tables: workers; groups: group-by;
SELECT COUNT(*) FROM workers HAVING SUM(salary * 2)/COUNT(*) > 0
