-- TPC-H/TPC-R Parts/Supplier Relationship Query (Q16)
-- Functional Query Definition
-- Approved February 1998
select
	p.brand,
	p.type,
	p.size,
	count(distinct ps.suppkey) as supplier_cnt
from
	partsupp as ps,
	part as p
where
	p.partkey = ps.partkey
	and p.brand <> 'Brand#45'
	and p.type not like 'MEDIUM POLISHED%'
	and p.size in (49, 14, 23, 45, 19, 3, 36, 9)
	and ps.suppkey not in (
		select
			suppkey
		from
			supplier
		where
			comment like '%Customer%Complaints%'
	)
group by
	p.brand,
	p.type,
	p.size
order by
	supplier_cnt desc,
	p.brand,
	p.type,
	p.size;
