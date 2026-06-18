-- database: presto; groups: aggregate; tables: datatype
select var_samp(distinct c_bigint), var_samp(distinct c_double) from datatype
