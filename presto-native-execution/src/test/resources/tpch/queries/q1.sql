-- TPC-H/TPC-R Pricing Summary Report Query (Q1)
-- Functional Query Definition
-- Approved February 1998
-- Fixed shipdate predicate as DWRF doesn't support DATE types
select returnflag,
--        linestatus,
--        avg(discount)                                   as avg_disc,
--        avg(extendedprice)                              as avg_price,
--        avg(quantity)                                   as avg_qty,
--

       sum(extendedprice) as sum_base_price,
       count(*)           as count_order

from lineitem
-- where cast(shipdate as date) >= date '1998-12-01' - interval '90' day
--   and shipdate <= '1998-12-01'
group by returnflag
--          ,
--          linestatus
order by returnflag
--          ,
--          linestatus