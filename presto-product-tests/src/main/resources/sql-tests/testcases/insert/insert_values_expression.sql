-- database: presto; groups: insert; mutable_tables: datatype|created; 
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.hive.datatype} values(5 * 10, 4.1 + 5, 'abc', cast('2014-01-01' as date), cast('2015-01-01 03:15:16 UTC' as timestamp), TRUE);
select * from ${mutableTables.hive.datatype}
--!
50|9.1|abc|2014-01-01|2015-01-01 03:15:16|true|
