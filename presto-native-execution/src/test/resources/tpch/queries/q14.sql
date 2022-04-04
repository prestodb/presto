-- TPC-H/TPC-R Promotion Effect Query (Q14)
-- Functional Query Definition
-- Approved February 1998
select
	100.00 * sum(case
		when p.type like 'PROMO%'
			then l.extendedprice * (1 - l.discount)
		else 0
	end) / sum(l.extendedprice * (1 - l.discount)) as promo_revenue
from
	lineitem as l,
	part as p
where
	l.partkey = p.partkey
	and l.shipdate >= date '1995-09-01'
	and l.shipdate < date '1995-09-01' + interval '1' month;
