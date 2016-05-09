-- database: presto; groups: insert; mutable_tables: datatype|created; tables: datatype
-- delimiter: |; ignoreOrder: true;
--!
insert into ${mutableTables.hive.datatype} select * from datatype order by 1 limit 2;
select * from ${mutableTables.hive.datatype}
--!
12|12.25|String1|1999-01-08|1999-01-08 02:05:06|true|123.22|12345678901234567890.0123456789|
25|55.52|test|1952-01-05|1989-01-08 04:05:06|false|321.21|-12345678901234567890.0123456789|
