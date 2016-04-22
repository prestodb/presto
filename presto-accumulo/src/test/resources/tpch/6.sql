-- $ID$
-- TPC-H/TPC-R Forecasting Revenue Change Query (Q6)
-- Functional Query Definition
-- Approved February 1998

select
	sum(l.extendedprice * l.discount) as revenue
from
	lineitem l
where
	l.shipdate >= date '1994-01-01'
	and l.shipdate < date '1994-01-01' + interval '1' year
	and l.discount between .06 - 0.01 and .06 + 0.01
	and l.quantity < 24