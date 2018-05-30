-- database: presto; groups: orderby, limit
select avg(retailprice), mfgr from tpch.tiny.part group by 2 order by count(*) limit 20
