-- database: presto; groups: limit; tables: nation
SELECT foo.c, foo.n_regionkey FROM
    (SELECT n_regionkey, COUNT(*) AS c FROM nation
    GROUP BY n_regionkey ORDER BY n_regionkey LIMIT 2) foo