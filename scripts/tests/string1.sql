select orderkey, linenumber, s1, s2, s3, s4 from hive.tpch.strings where
 s1 > 'f'
 and s2 > '1'
 and s3 > '1'
 and s4 > '2';
 
