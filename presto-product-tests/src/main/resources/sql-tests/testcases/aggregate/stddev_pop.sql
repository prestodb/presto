-- database: presto; groups: aggregate; tables: datatype
select stddev_pop(c_bigint), stddev_pop(c_double) from datatype
