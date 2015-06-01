-- database: presto; groups: insert; mutable_tables: datatype|created; tables: datatype
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.hive.datatype} select 1,2.1,'abc \n def', cast(null as date), cast(null as timestamp), cast(null as boolean) from datatype;
select * from ${mutableTables.hive.datatype}
--!
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
1|2.1|abc \n def|null|null|null|
