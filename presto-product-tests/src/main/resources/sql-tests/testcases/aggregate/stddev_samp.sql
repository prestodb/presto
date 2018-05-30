-- database: presto; groups: aggregate; tables: datatype
select stddev_samp(c_bigint), stddev_samp(c_double) from datatype
