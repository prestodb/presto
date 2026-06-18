-- database: presto; groups: join; tables: nation, workers
select n_name, department, name, salary from nation right outer join workers on n_nationkey = department
