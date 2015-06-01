-- database: presto; groups: limit; tables: nation
SELECT n_nationkey FROM nation WHERE n_name < 'INDIA'
ORDER BY n_nationkey LIMIT 3