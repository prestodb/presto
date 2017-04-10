-- database: presto; groups: aggregate; tables: datatype
select variance(c_bigint), variance(c_double) from datatype
