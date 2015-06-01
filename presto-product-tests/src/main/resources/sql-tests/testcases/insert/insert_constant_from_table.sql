-- database: presto; groups: insert; mutable_tables: datatype|created; tables: datatype
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.hive.datatype} select 1, 2.2, 'abc', cast('2014-01-01' as date), cast('2015-01-01 03:15:16 UTC' as timestamp), false from datatype;
select * from ${mutableTables.hive.datatype}
--!
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|
1|2.2|abc|2014-01-01|2015-01-01 03:15:16|false|

