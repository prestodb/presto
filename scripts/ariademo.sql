use hive.tpch;

-- aria enable Aria scan. Works with ORC V2 direct longs, doubles and direct Slices
set session aria=true;

-- aria_flags is a bitmask with:
-- 1: enable repartitioning
-- 2: enable hash join, works only with fixed size data, hash join w 2 longs k=as key and 1 double as payload.
-- 4 reuse buffers in ORC reader,
-- 8 reuse buffers in exchange,
-- 32 reuse Blocks in exchange.
-- 64 prune unreferenced struct/map/array fields. 

set session aria_flags = 47;
-- Enables reusing Pages and Blocks between result batches if the pipeline is suitable, e.g. scan - repartition - hash join
set session aria_reuse_pages = true;
-- Enables adaptive reorder of single column filters.
set session aria_reorder = true;


-- The literals are scaled  for 100G scale. The tables should be compressed with Snappy.
-- Note hat the Presto CLI shows a rate of rows/s. Aria counts rows after filtering, baseline counts rows before filtering.

-- 1/1M
select  sum (extendedprice) from lineitem_s where suppkey = 111;

-- 1/100 * 1/100
select count(*), sum (extendedprice), sum (quantity) from lineitem_s where  partkey between 10000000 and 10200000 and suppkey between 500000 and 510000; 


-- 1/5
select sum (partkey) from lineitem_s where quantity < 10;

-- 1/1
select max (orderkey), max(partkey), max(suppkey) from lineitem_s where suppkey > 0;



-- 1/5 * 1/10
select count (*), sum (l.extendedprice * (1 - l.discount) - l.quantity * p.supplycost) from hive.tpch.lineitem_s l, hive.tpch.partsupp p where l.partkey = p.partkey and l.suppkey = p.suppkey and p.availqty < 1000 and quantity < 10;

-- 1 * 1/10 
select count (*), sum (l.extendedprice * (1 - l.discount) - l.quantity * p.supplycost) from hive.tpch.lineitem_s l, hive.tpch.partsupp p where l.partkey = p.partkey and l.suppkey = p.suppkey and p.availqty < 1000;

-- 1 * 9/10 

select count (*), sum (l.extendedprice * (1 - l.discount) - l.quantity * p.supplycost) from hive.tpch.lineitem_s l, hive.tpch.partsupp p where l.partkey = p.partkey and l.suppkey = p.suppkey and p.availqty < 9000;


-- Example of filter reorder gains, from 58s cpu to 42s cpu
select count (*) from hive.tpch.lineitem_s where partkey < 19000000 and suppkey < 900000 and quantity < 45 and extendedprice < 9000;


-- Example of expression filter
select count (*), max(partkey) from hive.tpch.lineitem_s where partkey + 1 < 2000000;


select count (*), max(partkey) from hive.tpch.lineitem_s where comment like '%fur%' and partkey + 1 < 19000000 and suppkey < 100000;


select count (*), max(partkey) from hive.tpch.lineitem_s where comment like '%fur%' and partkey + 1 < 19000000 and suppkey < 100000;
select count (*), max(partkey) from hive.tpch.lineitem_s where comment like '%fur%' and partkey + 1 < 19000000 and suppkey + 1 < 100000;
select count (*), max(partkey) from hive.tpch.lineitem_s where comment like '%fur%' and partkey + 1 < 19000000 and suppkey + 1 < 100000 and suppkey + partkey < 2000000;


-- Errors


-- Example with mutually masking errors
select count (*) from hive.tpch.lineitem_s where
if (linenumber = 2, false, suppkey / (linenumber - 3) > 0)
and if (linenumber = 3, false, partkey / (linenumber - 2) > 0)
and orderkey < 1000000;




select count (*) from hive.tpch.lineitem_s where partkey between 1 and 10 or partkey between 100 and 200 or partkey between 300 and 400 or partkey between 800 and 900 or partkey between 1100 and 1200 or partkey between 1000000 and 1001000;


