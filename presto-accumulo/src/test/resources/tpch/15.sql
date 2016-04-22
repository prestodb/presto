-- $ID$
-- TPC-H/TPC-R Top Supplier Query (Q15)
-- Functional Query Definition
-- Approved February 1998

-- CREATE/DROP of the 'revenue' view is handled by the test suite

select
	s.suppkey,
	s.name,
	s.address,
	s.phone,
	r.revenue
from
	supplier s,
	revenue r
where
	s.suppkey = r.number
	and r.revenue = (
		select
			max(r.revenue)
		from revenue r
	)
order by
	s.suppkey