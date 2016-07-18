-- database: presto; groups: insert; mutable_tables: datatype|created; tables: datatype
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.hive.datatype} select 1, cast(2.1 as double), 'abc \n def', cast(null as date), cast(null as timestamp), cast(null as boolean), cast(null as decimal(5,2)), cast(null as decimal(30,10)) from datatype;
select * from ${mutableTables.hive.datatype}
--!
1|2.1|abc \n def|null|null|null|null|null|
1|2.1|abc \n def|null|null|null|null|null|
1|2.1|abc \n def|null|null|null|null|null|
1|2.1|abc \n def|null|null|null|null|null|
1|2.1|abc \n def|null|null|null|null|null|
1|2.1|abc \n def|null|null|null|null|null|
1|2.1|abc \n def|null|null|null|null|null|
1|2.1|abc \n def|null|null|null|null|null|
1|2.1|abc \n def|null|null|null|null|null|
1|2.1|abc \n def|null|null|null|null|null|
1|2.1|abc \n def|null|null|null|null|null|
1|2.1|abc \n def|null|null|null|null|null|
1|2.1|abc \n def|null|null|null|null|null|
1|2.1|abc \n def|null|null|null|null|null|
1|2.1|abc \n def|null|null|null|null|null|
1|2.1|abc \n def|null|null|null|null|null|
