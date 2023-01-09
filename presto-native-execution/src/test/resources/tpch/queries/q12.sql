-- TPC-H/TPC-R Shipping Modes and Order Priority Query (Q12)
-- Functional Query Definition
-- Approved February 1998
select
	l.shipmode,
	sum(case
		when o.orderpriority = '1-URGENT'
			or o.orderpriority = '2-HIGH'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o.orderpriority <> '1-URGENT'
			and o.orderpriority <> '2-HIGH'
			then 1
		else 0
	end) as low_line_count
from
	orders as o,
	lineitem as l
where
	o.orderkey = l.orderkey
	and l.shipmode in ('MAIL', 'SHIP')
	and l.commitdate < l.receiptdate
	and l.shipdate < l.commitdate
	and l.receiptdate >= date '1994-01-01'
	and l.receiptdate < date '1994-01-01' + interval '1' year
group by
	l.shipmode
order by
	l.shipmode;
