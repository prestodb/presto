-- database: presto; groups: aggregate; tables: datatype
select variance(distinct c_bigint), variance(distinct c_double) from datatype

