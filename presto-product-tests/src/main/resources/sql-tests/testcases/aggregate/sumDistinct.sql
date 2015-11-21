-- database: presto; groups: aggregate; tables: datatype
select sum(distinct c_bigint), sum(distinct c_double) from datatype
