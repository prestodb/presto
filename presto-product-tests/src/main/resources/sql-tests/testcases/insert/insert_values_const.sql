-- database: presto; groups: insert; mutable_tables: datatype|created; 
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.hive.datatype} values(1, cast(2.1 as double), 'abc', cast('2014-01-01' as date), cast('2015-01-01 03:15:16' as timestamp), FALSE, DECIMAL '-123.22', DECIMAL '-12345678901234567890.0123456789');
select * from ${mutableTables.hive.datatype}
--!
1|2.1|abc|2014-01-01|2015-01-01 03:15:16|false|-123.22|-12345678901234567890.0123456789|
