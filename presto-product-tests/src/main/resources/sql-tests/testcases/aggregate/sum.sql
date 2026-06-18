-- database: presto; groups: aggregate; tables: datatype
select sum(c_bigint), sum(c_double) from datatype
