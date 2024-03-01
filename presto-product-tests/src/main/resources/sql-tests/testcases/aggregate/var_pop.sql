-- database: presto; groups: aggregate; tables: datatype
select var_pop(c_bigint), var_pop(c_double) from datatype
