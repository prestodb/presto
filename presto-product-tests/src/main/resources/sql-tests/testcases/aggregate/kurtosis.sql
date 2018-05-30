-- database: presto; groups: aggregate; tables: datatype
select kurtosis(c_bigint), kurtosis(c_double) from datatype
