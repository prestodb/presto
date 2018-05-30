-- database: presto; groups: aggregate; tables: datatype
select max(upper(c_string)), min(upper(c_string)) from datatype
