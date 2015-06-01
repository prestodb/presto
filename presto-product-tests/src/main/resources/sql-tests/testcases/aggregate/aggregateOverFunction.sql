-- database: presto; groups: aggregate; tables: datatype
select max(upper(c_varchar)), min(upper(c_varchar)) from datatype
