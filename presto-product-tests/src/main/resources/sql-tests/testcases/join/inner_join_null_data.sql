-- database: presto; groups: join; tables: nation, workers
select n_name, department, name, salary from nation, workers where n_nationkey = department
