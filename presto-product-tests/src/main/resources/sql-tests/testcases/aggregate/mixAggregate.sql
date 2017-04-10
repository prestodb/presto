-- database: presto; groups: aggregate; tables: datatype
select count(c_string), max(c_double), avg(c_bigint) from datatype
