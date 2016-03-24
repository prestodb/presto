-- database: presto; groups: insert; mutable_tables: datatype|created; tables: datatype
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.hive.datatype} select count(*), 1.1, 'a', cast('2016-01-01' as date), cast('2015-01-01 03:15:16 UTC' as timestamp), FALSE from datatype group by c_bigint;
select * from ${mutableTables.hive.datatype}
--!
1|1.1|a|2016-01-01|2015-01-01 03:15:16|false|
1|1.1|a|2016-01-01|2015-01-01 03:15:16|false|
1|1.1|a|2016-01-01|2015-01-01 03:15:16|false|
1|1.1|a|2016-01-01|2015-01-01 03:15:16|false|
1|1.1|a|2016-01-01|2015-01-01 03:15:16|false|
1|1.1|a|2016-01-01|2015-01-01 03:15:16|false|
4|1.1|a|2016-01-01|2015-01-01 03:15:16|false|
1|1.1|a|2016-01-01|2015-01-01 03:15:16|false|
1|1.1|a|2016-01-01|2015-01-01 03:15:16|false|
4|1.1|a|2016-01-01|2015-01-01 03:15:16|false|
