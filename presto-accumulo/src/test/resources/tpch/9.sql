-- $ID$
-- TPC-H/TPC-R Product Type Profit Measure Query (Q9)
-- Functional Query Definition
-- Approved February 1998

select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n.name as nation,
			extract(year from o.orderdate) as o_year,
			l.extendedprice * (1 - l.discount) - ps.supplycost * l.quantity as amount
		from
			nation n2,
			region r,
			part p,
			supplier s,
			lineitem l,
			partsupp ps,
			orders o,
			nation n
		where
			s.suppkey = l.suppkey
			and ps.suppkey = l.suppkey
			and ps.partkey = l.partkey
			and p.partkey = l.partkey
			and o.orderkey = l.orderkey
			and s.nationkey = n.nationkey
			and p.name like '%green%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc