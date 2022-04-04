-- TPC-H/TPC-R Forecasting Revenue Change Query (Q6)
-- Functional Query Definition
-- Approved February 1998
select
	sum(extendedprice * discount) as revenue
from
	lineitem
where
	shipdate >= date '1994-01-01'
        and shipdate < date '1994-01-01' + interval '1' year
        and discount between decimal '0.06' - decimal '0.01' and decimal '0.06' + decimal '0.01'
	and quantity < 24;
