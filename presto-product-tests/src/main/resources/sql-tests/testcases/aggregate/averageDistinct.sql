-- database: presto; groups: aggregate; tables: datatype
select avg(distinct c_bigint), avg(distinct c_double) from datatype
