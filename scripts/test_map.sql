


-- Column index of numeric/date columns to column value as long.
create table hive.tpch.lineitem_map as
select orderkey as l_orderkey,
linenumber as l_linenumber,
map(
array[2, 3, 5, 6, 7, 8, 11, 12, 13,
if(receiptdate > commitdate, 100, 101),
if (returnflag = 'R', 102, 103),
104],

array[partkey, suppkey,
cast(quantity as bigint), cast (extendedprice as bigint), 
cast (discount * 100 as bigint), cast (tax * 100 as bigint), 
date_diff('day', cast('1970-1-1' as date), shipdate),
date_diff('day', cast('1970-1-1' as date), commitdate),
date_diff('day', cast('1970-1-1' as date), receiptdate),
date_diff('day', commitdate, receiptdate),
cast(extendedprice as bigint),
if (extendedprice > 5000, cast(extendedprice * discount as bigint), null)]
) as ints,
map(array['9', '10', '13', '14', '15'],
array[returnflag, linestatus, shipmode, shipinstruct, comment]) as strs  
from hive.tpch.lineitem;



-- 1.  orderkey      
-- 2.  partkey       
-- 3.  suppkey       
-- 4.  linenumber    
-- 5. quantity      
-- 6. extendedprice 
-- 7. discount      
-- 8.  tax           
-- 9.  returnflag    
-- 10.  linestatus    
-- 11.  shipdate      
-- 12.  commitdate    
-- 13.  receiptdate   
-- 14.  shipinstruct  
-- 15.  shipmode      
-- 16.  comment       
