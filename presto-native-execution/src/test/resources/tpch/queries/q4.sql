-- TPC-H/TPC-R Order Priority Checking Query (Q4)
-- Functional Query Definition
-- Approved February 1998
select
	o.orderpriority,
	count(*) as order_count
from
	orders as o
where
	o.orderdate >= date '1993-07-01'
	and o.orderdate < date '1993-07-01' + interval '3' month
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
