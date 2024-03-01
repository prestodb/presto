-- TPC-H/TPC-R Important Stock Identification Query (Q11)
-- Functional Query Definition
-- Approved February 1998
select
	ps.partkey,
	sum(ps.supplycost * ps.availqty) as value
from
	partsupp as ps,
	supplier as s,
	nation as n
where
	ps.suppkey = s.suppkey
	and s.nationkey = n.nationkey
	and n.name = 'GERMANY'
group by
	ps.partkey
having
    sum(ps.supplycost * ps.availqty) > (
        select
            sum(ps.supplycost * ps.availqty) * 0.0001
        from
            partsupp as ps,
            supplier as s,
            nation as n
        where
            ps.suppkey = s.suppkey
            and s.nationkey = n.nationkey
            and n.name = 'GERMANY'
    )
order by value desc;
