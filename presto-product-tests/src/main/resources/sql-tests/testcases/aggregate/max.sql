-- database: presto; groups: aggregate; tables: datatype
select max(c_bigint), max(c_double),max(c_string), max(c_date), max(c_timestamp) from datatype
