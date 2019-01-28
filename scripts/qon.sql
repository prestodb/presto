
-- Different configurations of Queen of the Night

-- Baseline
set session aria=false;
set session aria_reuse_pages=false;
set session aria_flags = 0;

select count (*), sum (l.extendedprice * (1 - l.discount) - l.quantity * p.supplycost) from hive.tpch.lineitem_s l, hive.tpch.partsupp p where l.partkey = p.partkey and l.suppkey = p.suppkey and p.availqty < 1000;


-- Hash join
set session aria_flags = 1;
select count (*), sum (l.extendedprice * (1 - l.discount) - l.quantity * p.supplycost) from hive.tpch.lineitem_s l, hive.tpch.partsupp p where l.partkey = p.partkey and l.suppkey = p.suppkey and p.availqty < 1000;


-- Repartition
set session aria_flags = 3;
select count (*), sum (l.extendedprice * (1 - l.discount) - l.quantity * p.supplycost) from hive.tpch.lineitem_s l, hive.tpch.partsupp p where l.partkey = p.partkey and l.suppkey = p.suppkey and p.availqty < 1000;


-- Exchange
set session aria_flags = 11;
select count (*), sum (l.extendedprice * (1 - l.discount) - l.quantity * p.supplycost) from hive.tpch.lineitem_s l, hive.tpch.partsupp p where l.partkey = p.partkey and l.suppkey = p.suppkey and p.availqty < 1000;


-- Aria scan
set session aria = true;
set session aria_flags = 15;
select count (*), sum (l.extendedprice * (1 - l.discount) - l.quantity * p.supplycost) from hive.tpch.lineitem_s l, hive.tpch.partsupp p where l.partkey = p.partkey and l.suppkey = p.suppkey and p.availqty < 1000;


-- Interoperator Page/Block memory reuse
set session aria_reuse_pages = true;
set session aria_flags = 47;
select count (*), sum (l.extendedprice * (1 - l.discount) - l.quantity * p.supplycost) from hive.tpch.lineitem_s l, hive.tpch.partsupp p where l.partkey = p.partkey and l.suppkey = p.suppkey and p.availqty < 1000;



