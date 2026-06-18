-- TPC-H/TPC-R Customer Distribution Query (Q13)
-- Functional Query Definition
-- Approved February 1998
select
	c_count,
	count(*) as custdist
from
	(
		select
			c.custkey,
			count(o.orderkey)
		from
			customer c left outer join orders o on
				c.custkey = o.custkey
				and o.comment not like '%special%requests%'
		group by
			c.custkey
	) as c_orders (c_custkey, c_count)
group by
	c_count
order by
	custdist desc,
	c_count desc;
