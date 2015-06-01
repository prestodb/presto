-- database: presto; groups: no_from
SELECT MIN(10), 3 as col1 GROUP BY 2  HAVING 6 > 5 ORDER BY 1