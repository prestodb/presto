-- TPC-H/TPC-R Forecasting Revenue Change Query (Q6)
-- Functional Query Definition
-- Approved February 1998
-- Fixed date predicates as DWRF doesn't support DATE types
select
	sum(extendedprice * discount) as revenue
from
	lineitem
where
	cast(shipdate as date) >= date '1994-01-01'
        and cast(shipdate as date) < date '1994-01-01' + interval '1' year
        and discount between decimal '0.06' - decimal '0.01' and decimal '0.06' + decimal '0.01'
	and quantity < 24;
