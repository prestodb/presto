-- TPC-H/TPC-R Top Supplier Query (Q15)
-- Functional Query Definition
-- Approved February 1998

with revenue as (
select
    suppkey as supplier_no,
    sum(extendedprice * (1 - discount)) as total_revenue
from
    lineitem
where
    shipdate >= date '1996-01-01'
    and shipdate < date '1996-01-01' + interval '3' month
group by suppkey
)

select
	su.suppkey,
	su.name,
	su.address,
	su.phone,
	total_revenue
from
	supplier as su,
	revenue
where
	su.suppkey = supplier_no
	and total_revenue = (
		select
			max(total_revenue)
		from
			revenue
	)
order by
	su.suppkey;
