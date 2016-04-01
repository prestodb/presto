-- database: presto; groups: insert; mutable_tables: datatype|created; tables: datatype
-- delimiter: |;
--!
insert into ${mutableTables.hive.datatype} values (null, null, null, null, null, null);
select * from ${mutableTables.hive.datatype}
--!
null|null|null|null|null|null|
