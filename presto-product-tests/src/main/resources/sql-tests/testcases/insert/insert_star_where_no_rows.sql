-- database: presto; groups: insert; mutable_tables: datatype|created; tables: datatype
-- delimiter: |; ignoreOrder: true; 
--!
insert into ${mutableTables.hive.datatype} select * from datatype where c_bigint < 0;
select * from ${mutableTables.hive.datatype}
--!
