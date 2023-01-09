-- TPC-H/TPC-R Shipping Priority Query (Q3)
-- Functional Query Definition
-- Approved February 1998
select
	l.orderkey,
	sum(l.extendedprice * (1 - l.discount)) as revenue,
	o.orderdate,
	o.shippriority
from
	customer as c,
	orders as o,
	lineitem as l
where
	c.mktsegment = 'BUILDING'
	and c.custkey = o.custkey
	and l.orderkey = o.orderkey
	and o.orderdate < date '1995-03-15'
	and l.shipdate > date '1995-03-15'
group by
	l.orderkey,
	o.orderdate,
	o.shippriority
order by
	revenue desc,
	o.orderdate
limit 10;
