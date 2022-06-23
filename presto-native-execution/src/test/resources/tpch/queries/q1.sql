-- TPC-H/TPC-R Pricing Summary Report Query (Q1)
-- Functional Query Definition
-- Approved February 1998
-- Fixed shipdate predicate as DWRF doesn't support DATE types
select
	returnflag,
	linestatus,
	sum(quantity) as sum_qty,
	sum(extendedprice) as sum_base_price,
	sum(extendedprice * (1 - discount)) as sum_disc_price,
	sum(extendedprice * (1 - discount) * (1 + tax)) as sum_charge,
	avg(quantity) as avg_qty,
	avg(extendedprice) as avg_price,
	avg(discount) as avg_disc,
	count(*) as count_order
from
	lineitem
where
    shipdate >= date '1998-12-01' - interval '90' day
    and shipdate <= date '1998-12-01'
group by
	returnflag,
	linestatus
order by
	returnflag,
	linestatus;
