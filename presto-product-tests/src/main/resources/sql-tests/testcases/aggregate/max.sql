-- database: presto; groups: aggregate; tables: datatype
select max(c_bigint), max(c_double),max(c_varchar), max(c_date), max(c_timestamp) from datatype
