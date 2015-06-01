-- database: presto; tables: empty; groups: empty;
SELECT SUM(cnt) FROM (SELECT COUNT(*) AS cnt FROM empty) foo
