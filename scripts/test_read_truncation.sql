

create table hive.tpch.strings as select
 orderkey, linenumber, comment as s1,
 concat (cast(partkey as varchar), comment) as s2,
        concat(cast(suppkey as varchar), comment) as s3,
                            concat(cast(quantity as varchar), comment) as s4
                            from hive.tpch.lineitem_s where orderkey < 100000;


create table hive.tpch.strings2 as select
 orderkey, linenumber,
 cast (row(comment,
 concat (cast(partkey as varchar), comment))
   as row(s1 varchar, s2 varchar)) as s1,
        cast (row(concat(cast(suppkey as varchar), comment),
                            concat(cast(quantity as varchar), comment))
                              as row(s3 varchar, s4 varchar)) as s3
                            from hive.tpch.lineitem_s where orderkey < 100000;


select orderkey, linenumber, s1, s2, s3, s4 from hive.tpch.strings where
 s1 > 'f'
 and s2 > '1'
 and s3 > '1'
 and s4 > '2';

select orderkey, linenumber, s1.s1, s1.s2, s3.s3, s4.s4 from hive.tpch.strings2 where
 s1.s1 > 'f'
 and s1.s2 > '1'
 and s3.s3 > '1'
 and s3.s4 > '2';


select orderkey, linenumber, s1.s1, s1.s2, s3.s3, s4.s4 from hive.tpch.strings2 where
 s1.s1 > 'f';
 




