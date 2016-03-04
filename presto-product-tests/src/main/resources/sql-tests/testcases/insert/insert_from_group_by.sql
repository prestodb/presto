-- database: presto; groups: insert; mutable_tables: datatype|created; tables: datatype
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.hive.datatype} select count(*), cast(1.1 as double), 'a', cast('2016-01-01' as date), cast('2015-01-01 03:15:16 UTC' as timestamp), FALSE, DECIMAL '-123.22', DECIMAL '-12345678901234567890.0123456789' from datatype group by c_bigint;
select * from ${mutableTables.hive.datatype}
--!
1|1.1|a|2016-01-01|2015-01-01 03:15:16|false|-123.22|-12345678901234567890.0123456789|
1|1.1|a|2016-01-01|2015-01-01 03:15:16|false|-123.22|-12345678901234567890.0123456789|
1|1.1|a|2016-01-01|2015-01-01 03:15:16|false|-123.22|-12345678901234567890.0123456789|
1|1.1|a|2016-01-01|2015-01-01 03:15:16|false|-123.22|-12345678901234567890.0123456789|
1|1.1|a|2016-01-01|2015-01-01 03:15:16|false|-123.22|-12345678901234567890.0123456789|
1|1.1|a|2016-01-01|2015-01-01 03:15:16|false|-123.22|-12345678901234567890.0123456789|
4|1.1|a|2016-01-01|2015-01-01 03:15:16|false|-123.22|-12345678901234567890.0123456789|
1|1.1|a|2016-01-01|2015-01-01 03:15:16|false|-123.22|-12345678901234567890.0123456789|
1|1.1|a|2016-01-01|2015-01-01 03:15:16|false|-123.22|-12345678901234567890.0123456789|
4|1.1|a|2016-01-01|2015-01-01 03:15:16|false|-123.22|-12345678901234567890.0123456789|
