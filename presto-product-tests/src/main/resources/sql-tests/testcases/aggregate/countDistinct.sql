-- database: presto; groups: aggregate; tables: datatype
select count(distinct c_bigint),count(distinct c_double),count(distinct c_string),count(distinct c_date),count(distinct c_timestamp),count(distinct c_boolean) from datatype
