-- database: presto; groups: aggregate; tables: datatype
select stddev_pop(distinct c_bigint), stddev_pop(distinct c_double) from datatype
