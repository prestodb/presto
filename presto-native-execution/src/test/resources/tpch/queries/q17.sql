-- TPC-H/TPC-R Small-Quantity-Order Revenue Query (Q17)
-- Functional Query Definition
-- Approved February 1998
select
	sum(l.extendedprice) / 7.0 as avg_yearly
from
	lineitem as l,
	part as p
where
	p.partkey = l.partkey
	and p.brand = 'Brand#23'
	and p.container = 'MED BOX'
	and l.quantity < (
		select
			0.2 * avg(quantity)
		from
			lineitem
		where
			partkey = p.partkey
	);
