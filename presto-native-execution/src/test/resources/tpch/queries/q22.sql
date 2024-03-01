-- TPC-H/TPC-R Global Sales Opportunity Query (Q22)
-- Functional Query Definition
-- Approved February 1998
select
	cntrycode,
	count(*) as numcust,
	sum(c_acctbal) as totacctbal
from
	(
		select
			substring(phone from 1 for 2) as cntrycode,
			acctbal as c_acctbal
		from
			customer
		where
			substring(phone from 1 for 2) in
                ('13', '31', '23', '29', '30', '18', '17')
			and acctbal > (
				select
					avg(acctbal)
				from
					customer
				where
					acctbal > 0.00
					and substring(phone from 1 for 2) in
                        ('13', '31', '23', '29', '30', '18', '17')
			)
			and not exists (
				select
					*
				from
					orders as o
				where
					o.custkey = custkey
			)
	) as custsale
group by
	cntrycode
order by
	cntrycode;
