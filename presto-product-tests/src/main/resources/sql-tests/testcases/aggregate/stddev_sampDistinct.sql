-- database: presto; groups: aggregate; tables: datatype
select stddev_samp(distinct c_bigint), stddev_samp(distinct c_double) from datatype
