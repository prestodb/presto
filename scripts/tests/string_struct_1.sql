select orderkey, linenumber, s1.s1, s1.s2, s3.s3, s3.s4 from hive.tpch.strings2 where
 s1.s1 > 'f'
;

