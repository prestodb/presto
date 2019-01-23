

-- custkey with map from year to an array of purchases that year.


create table hive.tpch.cust_year_parts as select custkey, map_agg(y, parts) as year_parts, map_agg(y, total_cost) as year_cost
  from (select c.custkey, year(shipdate) as y, array_agg(cast (row (partkey, extendedprice, quantity) as row (pk bigint, ep double, qt double))) as parts, sum (extendedprice) as total_cost
  from hive.tpch.lineitem_s l, hive.tpch.orders o, hive.tpch.customer c where l.orderkey = o.orderkey and o.custkey = c.custkey and c.nationkey = 1 and quantity < 10
  group by c.custkey, year(shipdate))
  group by custkey;



  

create table hive.tpch.exportlineitem as
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
from hive.tpch.lineitem l, hive.tpch.orders o, hive.tpch.customer c, hive.tpch.supplier s, hive.tpch.part p
where l.orderkey = o.orderkey and l.partkey = p.partkey and l.suppkey = s.suppkey and c.custkey = o.custkey; 



select l.applydate from (select e.license as l from (select export as e from hive.tpch.exportinfo where orderkey < 5));

select orderkey, linenumber, s_nation, l.applydate from (select orderkey, linenumber, e.s_nation, e.license as l from (select orderkey, linenumber, export as e from hive.tpch.exportinfo where orderkey < 15));




create table hive.tpch.cust_order_line as
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
from hive.tpch.lineitem l,
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
from hive.tpch.orders o, hive.tpch.customer c
where 
o.custkey = c.custkey and c.custkey between 0 and 2000000)
where o_orderkey = l.orderkey  
group by c_custkey, o_orderkey)
group by c_custkey;


select count (*) from    hive.tpch.cust_order_line cross join unnest c_orders cross join unnest o_lines where l_partkey = 111111;


select count (*) from hive.tpch.exportlineitem where export.s_nation = 2;
