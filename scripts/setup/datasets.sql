set session aria_scan = false;

create table hive.tpch.lineitem1
   as
select *,
          map(array[1, 2, 3], array[l_orderkey, l_partkey, l_suppkey]) as l_map,
           array[l_orderkey, l_partkey, l_suppkey] as l_array
from (
select  orderkey as l_orderkey,
partkey as l_partkey,
suppkey as l_suppkey,
linenumber as l_linenumber,
quantity as l_quantity,
 extendedprice as l_extendedprice,
shipmode as l_shipmode,
 comment as l_comment,
returnflag = 'R' as is_returned,
 cast (quantity + 1 as real) as l_floatQuantity
  from tpch.sf1.lineitem);



--

-- Alternating stretches of non-nulls for 99K rows followed by 99K rows where non-key columns have nulls every few rows.

create table hive.tpch.lineitem1_nulls
   as
select *,
        if (mod (l_orderkey, 198000) > 99000 and mod(l_orderkey + l_linenumber, 5) = 0, null,
          map(array[1, 2, 3], array[l_orderkey, l_partkey, l_suppkey])) as l_map,
        if (mod (l_orderkey, 198000) > 99000 and mod(l_orderkey + l_linenumber, 5) = 0, null,
           array[l_orderkey, l_partkey, l_suppkey]) as l_array

from (
select  orderkey as l_orderkey,
  if (have_nulls and mod(orderkey + linenumber, 11) = 0, null, partkey) as l_partkey,
  if (have_nulls and mod(orderkey + linenumber, 13) = 0, null, suppkey) as l_suppkey,
linenumber as l_linenumber,
  if (have_nulls and mod (orderkey + linenumber, 17) = 0, null, quantity) as l_quantity,
  if (have_nulls and mod (orderkey + linenumber, 19) = 0, null, extendedprice) as l_extendedprice,
  if (have_nulls and mod (orderkey + linenumber, 23) = 0, null, shipmode) as l_shipmode,
  if (have_nulls and mod (orderkey + linenumber, 7) = 0, null, comment) as l_comment,
    if (have_nulls and mod(orderkey + linenumber, 31) = 0, null, returnflag = 'R') as is_returned,
      if (have_nulls and mod(orderkey + linenumber, 37) = 0, null, cast (quantity + 1 as real)) as l_floatQuantity
  from (select mod (orderkey, 198000) > 99000 as have_nulls, * from tpch.sf1.lineitem));
  

create table hive.tpch.lineitem1_struct as
select l.orderkey as l_orderkey, linenumber as l_linenumber,
cast (row (
l.partkey, l.suppkey, extendedprice, discount, quantity, shipdate, receiptdate, commitdate, l.comment)
as row(
l_partkey bigint, l_suppkey bigint, l_extendedprice double, l_discount double, l_quantity double, l_shipdate date, l_receiptdate date, l_commitdate date, l_comment varchar(44)
)) as l_shipment,
  CASE WHEN S.nationkey = C.nationkey THEN NULL ELSE 
CAST (row(
S.NATIONKEY, C.NATIONKEY,
CASE WHEN (S.NATIONKEY IN (6, 7, 19) AND C.NATIONKEY IN (6,7,19)) THEN 1 ELSE 0 END,
case when s.nationkey = 24 and c.nationkey = 10 then 1 else 0 end,
 case when p.comment like '%fur%' or p.comment like '%care%'
 then row(o.orderdate, l.shipdate, l.partkey + l.suppkey, concat(p.comment, l.comment))
   else null end
)
AS ROW (
s_nation bigint, c_nation bigint,
is_inside_eu int,
is_restricted int,
license row (applydate date, grantdate date, filing_no bigint, comment varchar)))
end as l_export
from tpch.sf1.lineitem l, tpch.sf1.orders o, tpch.sf1.customer c, tpch.sf1.supplier s, tpch.sf1.part p
where l.orderkey = o.orderkey and l.partkey = p.partkey and l.suppkey = s.suppkey and c.custkey = o.custkey; 



create table hive.tpch.cust_order_line1 as
select c_custkey, max(c_name) as c_name, max(c_address) as c_address, max(c_nationkey) as c_nationkey, max(c_phone) as c_phone, max(c_acctbal) as c_acctbal, max(c_mktsegment) as c_mktsegment, max(c_comment) as c_comment,
array_agg(
  cast (row (o_orderkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_shippriority,
 o_clerk, o_comment, lines)
 as row(
o_orderkey bigint, o_orderstatus varchar, o_totalprice double, o_orderdate date, o_orderpriority varchar, o_shippriority varchar,
 o_clerk varchar, o_comment varchar,
 o_lines array (
row (
 l_partkey  bigint   ,
 l_suppkey  bigint   ,
 l_linenumber   integer   ,
 l_quantity double   ,
 l_extendedprice  double   ,
 l_discount double   ,
 l_tax double   ,
 l_returnflag   varchar(1) ,
 l_linestatus   varchar(1) ,
 l_shipdate date ,
 l_commitdate   date ,
 l_receiptdate   date ,
 l_shipinstruct  varchar(25) ,
 l_shipmode varchar(10), 
 l_comment  varchar(44) )
)
))) as c_orders
 from (
select c_custkey as c_custkey, o_orderkey, max(c_name) as c_name, max(c_address) as c_address, max (c_nationkey) as c_nationkey, max(c_phone) as c_phone, max(c_acctbal) as c_acctbal, max(c_mktsegment) as c_mktsegment, max(c_comment) as c_comment,
max (o_orderstatus) as o_orderstatus, max(o_totalprice) as o_totalprice,   max(o_orderdate) as o_orderdate,
  max(o_orderpriority) as o_orderpriority, 
 max (o_clerk) as o_clerk,
 max(o_shippriority) as o_shippriority,
 max (o_comment) as o_comment, 
  array_agg(cast (row(
 l.partkey,
 l.suppkey,
 l.linenumber,
 l.quantity,
 l.extendedprice,
 l.discount,
 l.tax,
 l.returnflag,
 l.linestatus,
 l.shipdate,
 l.commitdate,
 l.receiptdate,
 l.shipinstruct,
 l.shipmode,
 l.comment
)
  as row (
 l_partkey  bigint   ,
 l_suppkey  bigint   ,
 l_linenumber   integer   ,
 l_quantity double   ,
 l_extendedprice  double   ,
 l_discount double   ,
 l_tax double   ,
 l_returnflag   varchar(1) ,
 l_linestatus   varchar(1) ,
 l_shipdate date ,
 l_commitdate   date ,
 l_receiptdate   date ,
 sl_hipinstruct  varchar(25) ,
 l_shipmode varchar(10), 
 l_comment  varchar(44) ))) as lines
from tpch.sf1.lineitem l,
  (select 
 c.custkey as c_custkey, 
 name     as c_name,
 address  as c_address,
 nationkey as c_nationkey,
 phone as c_phone,   
 acctbal as c_acctbal,  
 mktsegment as c_mktsegment, 
 c.comment as c_comment,  
 orderkey    as o_orderkey,
 orderstatus as o_orderstatus,  
 totalprice as o_totalprice,  
 orderdate as o_orderdate,   
 orderpriority as o_orderpriority, 
 clerk as o_clerk,     
 shippriority as o_shippriority, 
 o.comment as o_comment    
from tpch.sf1.orders o, tpch.sf1.customer c
where 
o.custkey = c.custkey and c.custkey between 0 and 2000000)
where o_orderkey = l.orderkey  
group by c_custkey, o_orderkey)
group by c_custkey;

create table hive.tpch.strings as select
 orderkey, linenumber, comment as s1,
 concat (cast(partkey as varchar), comment) as s2,
        concat(cast(suppkey as varchar), comment) as s3,
                            concat(cast(quantity as varchar), comment) as s4
                            from tpch.sf1.lineitem where orderkey < 100000;

create table hive.tpch.strings_struct as select
 orderkey, linenumber,
 cast (row(comment,
 concat (cast(partkey as varchar), comment))
   as row(s1 varchar, s2 varchar)) as s1,
        cast (row(concat(cast(suppkey as varchar), comment),
                            concat(cast(quantity as varchar), comment))
                              as row(s3 varchar, s4 varchar)) as s3
                            from tpch.sf1.lineitem where orderkey < 100000;

create table hive.tpch.strings_struct_nulls as select
 orderkey, linenumber,
 cast (if (mod(partkey, 5) = 0, null, row(comment,
 if (mod(partkey, 13) = 0, null,  concat (cast(partkey as varchar), comment))))
   as row(s1 varchar, s2 varchar)) as s1,
        cast (if (mod (partkey, 7) = 0, null,
           row(if (mod(suppkey, 17) = 0, null, concat(cast(suppkey as varchar), comment)),
                            concat(cast(quantity as varchar), comment)))
                              as row(s3 varchar, s4 varchar)) as s3
                            from hive.tpch.lineitem_s where orderkey < 100000;

