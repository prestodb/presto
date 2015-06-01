-- database: presto; groups: aggregate; tables: datatype
select count(c_bigint),count(c_double),count(c_varchar),count(c_date),count(c_timestamp),count(c_boolean) from datatype
