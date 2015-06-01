-- database: presto; groups: insert; mutable_tables: datatype|created; tables: datatype
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.hive.datatype} select 1, c_double, 'abc', cast('2014-01-01' as date), c_timestamp, FALSE from datatype;
select * from ${mutableTables.hive.datatype}
--!
1|12.25|abc|2014-01-01|1999-01-08 02:05:06|false|
1|55.52|abc|2014-01-01|1989-01-08 04:05:06|false|
1|0.245|abc|2014-01-01|2005-01-09 04:05:06|false|
1|12.25|abc|2014-01-01|2002-01-07 01:05:06|false|
1|99.8777|abc|2014-01-01|2010-01-02 04:03:06|false|
1|12.25|abc|2014-01-01|2010-01-02 04:03:06|false|
1|9.8777|abc|2014-01-01|2010-01-02 05:09:06|false|
1|98.8777|abc|2014-01-01|2010-01-02 04:03:06|false|
1|12.8788|abc|2014-01-01|2010-01-02 02:05:06|false|
1|67.87|abc|2014-01-01|2010-01-02 04:03:06|false|
1|67.87|abc|2014-01-01|2010-01-02 04:03:06|false|
1|67.87|abc|2014-01-01|2010-01-02 04:03:06|false|
1|67.87|abc|2014-01-01|2010-01-02 04:03:06|false|
1|67.87|abc|2014-01-01|2010-01-02 04:03:06|false|
1|null|abc|2014-01-01|null|false|
1|98.52|abc|2014-01-01|null|false|

