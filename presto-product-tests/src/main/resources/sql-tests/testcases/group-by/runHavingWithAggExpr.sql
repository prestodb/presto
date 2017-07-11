-- database: presto; tables: workers; groups: group-by;
SELECT COUNT(*) FROM workers HAVING SUM(salary * 2)/COUNT(*) > 0
