-- database: presto; groups: join; tables: nation, part
select n_name, p_name from nation left outer join part on n_regionkey = p_partkey and n_name = p_name
