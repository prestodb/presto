-- database: presto; requires: com.facebook.presto.tests.ImmutableTpchTablesRequirements; tables: workers; groups: group-by;
SELECT id_department, COUNT(*) FROM workers GROUP BY id_department HAVING COUNT(*) > 1 ORDER BY id_department desc
