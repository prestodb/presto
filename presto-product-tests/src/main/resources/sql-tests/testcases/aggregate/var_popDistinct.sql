-- database: presto; groups: aggregate; tables: datatype
select var_pop(distinct c_bigint), var_pop(distinct c_double) from datatype
