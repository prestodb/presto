-- database: presto; groups: insert; mutable_tables: datatype|created; 
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.hive.datatype}
values(5 * 10, cast(4.1 + 5 as double), 'abc', cast('2014-01-01' as date), cast('2015-01-01 03:15:16' as timestamp), TRUE, DECIMAL '123.22', DECIMAL '12345678901234567890.0123456789');
select * from ${mutableTables.hive.datatype}
--!
50|9.1|abc|2014-01-01|2015-01-01 03:15:16|true|123.22|12345678901234567890.0123456789|
