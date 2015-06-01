-- database: presto; groups: qe, string_functions
select name from tpch.tiny.nation where name like '%AN'
