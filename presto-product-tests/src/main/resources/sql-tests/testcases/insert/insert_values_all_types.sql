-- database: presto; groups: insert; mutable_tables: datatype|created
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.hive.datatype} values(1,cast(2.34567 as double),'a',cast('2014-01-01' as date), cast ('2015-01-01 03:15:16' as timestamp), TRUE, DECIMAL '123.22', DECIMAL '12345678901234567890.0123456789');
select * from ${mutableTables.hive.datatype}
--!
1|2.34567|a|2014-01-01|2015-01-01 03:15:16|true|123.22|12345678901234567890.0123456789|
