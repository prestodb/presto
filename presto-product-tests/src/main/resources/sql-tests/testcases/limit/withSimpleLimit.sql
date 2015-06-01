-- database: presto; groups: limit; tables: nation
SELECT n_nationkey from nation ORDER BY n_nationkey DESC LIMIT 5