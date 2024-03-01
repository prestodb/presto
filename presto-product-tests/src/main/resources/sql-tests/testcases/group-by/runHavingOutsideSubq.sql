-- database: presto; tables: nation; groups: group-by;
SELECT n_regionkey FROM (SELECT n_regionkey, COUNT(*) cnt FROM nation GROUP BY n_regionkey) t GROUP BY n_regionkey HAVING n_regionkey < 3 AND COUNT(cnt) > 0
