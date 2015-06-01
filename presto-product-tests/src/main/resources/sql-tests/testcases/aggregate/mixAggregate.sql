-- database: presto; groups: aggregate; tables: datatype
select count(c_varchar), max(c_double), avg(c_bigint) from datatype
