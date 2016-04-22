-- $ID$
-- TPC-H/TPC-R Minimum Cost Supplier Query (Q2)
-- Functional Query Definition
-- Approved February 1998

select
	s.acctbal,
	s.name,
	n.name,
	p.partkey,
	p.mfgr,
	s.address,
	s.phone,
	s.comment
from
	part p,
	supplier s,
	partsupp ps,
	nation n,
	region r
where
	p.partkey = ps.partkey
	and s.suppkey = ps.suppkey
	and p.size = 15
	and p.type like '%BRASS'
	and s.nationkey = n.nationkey
	and n.regionkey = r.regionkey
	and r.name = 'EUROPE'
	and ps.supplycost = (
                        select
                        min(ps.supplycost)
                      from
                        part p,
                        partsupp ps,
                        supplier s,
                        nation n,
                        region r
                      where
                        p.partkey = ps.partkey
                        and s.suppkey = ps.suppkey
                        and s.nationkey = n.nationkey
                        and n.regionkey = r.regionkey
                        and r.name = 'EUROPE'
                      )
order by
	s.acctbal desc,
	n.name,
	s.name,
	p.partkey