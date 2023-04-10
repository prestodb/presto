-- TPC-H/TPC-R Order Priority Checking Query (Q4)
-- Functional Query Definition
-- Approved February 1998
-- Fixed date predicates as DWRF doesn't support DATE types
select
	o.orderpriority,
	count(*) as order_count
from
	orders as o
where
	cast(o.orderdate as date) >= date '1993-07-01'
	and cast(o.orderdate as date) < date '1993-07-01' + interval '3' month
	and exists (
		select
			*
		from
			lineitem as l
		where
			l.orderkey = o.orderkey
			and l.commitdate < l.receiptdate
	)
group by
	o.orderpriority
order by
	o.orderpriority;
