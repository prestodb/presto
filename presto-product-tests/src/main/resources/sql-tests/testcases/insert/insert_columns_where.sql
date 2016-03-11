-- database: presto; groups: insert; mutable_tables: datatype|created; tables: datatype
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.hive.datatype} select c_bigint, c_double, c_string, c_date, c_timestamp, c_boolean from datatype where c_double < 20;
select * from ${mutableTables.hive.datatype}
--!
12|12.25|String1|1999-01-08|1999-01-08 02:05:06|true|
964|0.245|Again|1936-02-08|2005-01-09 04:05:06|false|
100|12.25|testing|1949-07-08|2002-01-07 01:05:06|true|
5252|12.25|sample|1987-04-09|2010-01-02 04:03:06|true|
100|9.8777|STRING1|1923-04-08|2010-01-02 05:09:06|true|
100|12.8788|string1|1922-04-02|2010-01-02 02:05:06|true|

