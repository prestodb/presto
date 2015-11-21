-- database: presto; groups: aggregate; tables: datatype
select var_samp(c_bigint), var_samp(c_double) from datatype
