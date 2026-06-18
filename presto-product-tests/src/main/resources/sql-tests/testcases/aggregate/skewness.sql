-- database: presto; groups: aggregate; tables: datatype
select skewness(c_bigint), skewness(c_double) from datatype
