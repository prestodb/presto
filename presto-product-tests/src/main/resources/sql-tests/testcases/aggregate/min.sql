-- database: presto; groups: aggregate; tables: datatype
select min(c_bigint), min(c_double), min(c_varchar), min(c_date), min(c_timestamp) from datatype
