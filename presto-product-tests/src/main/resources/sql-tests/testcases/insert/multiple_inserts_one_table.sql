-- database: presto; groups: insert; mutable_tables: datatype|created; 
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.hive.datatype} values(1, cast(2.1 as double), 'abc', cast('2014-01-01' as date), cast('2015-01-01 03:15:16' as timestamp), FALSE, DECIMAL '-999.99', DECIMAL '-99999999999999999999.9999999999');
insert into ${mutableTables.hive.datatype} values(1, cast(2.1 as double), 'abc', cast('2014-01-01' as date), cast('2015-01-01 03:15:16' as timestamp), FALSE, DECIMAL '999.99', DECIMAL '99999999999999999999.9999999999');
insert into ${mutableTables.hive.datatype} values(1, cast(2.1 as double), 'abc', cast('2014-01-01' as date), cast('2015-01-01 03:15:16' as timestamp), FALSE, CAST(0 AS DECIMAL(5,2)), CAST(0 AS DECIMAL(30,10)));
select * from ${mutableTables.hive.datatype}
--!
1|2.1|abc|2014-01-01|2015-01-01 03:15:16|false|-999.99|-99999999999999999999.9999999999|
1|2.1|abc|2014-01-01|2015-01-01 03:15:16|false|999.99|99999999999999999999.9999999999|
1|2.1|abc|2014-01-01|2015-01-01 03:15:16|false|000.00|00000000000000000000.0000000000|
