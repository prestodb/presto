--

-- Alternating stretches of non-nulls for 99K rows followed by 99K rows where non-key columns have nulls every few rows.

create table hive.tpch.lineitem_nulls
--(l_orderkey bigint, l_partkey bigint, l_suppkey bigint,  l_linenumber int, l_quantity double, l_extendedprice double, l_shipmode varchar(10), l_comment varchar(44),
--  l_returned boolean,
--    l_floatquantity float,
--      l_map map(bigint, bigint),
--        l_array array bigint
--    )
    
as
select *,
        if (mod (l_orderkey, 198000) > 99000 and mod(l_orderkey + l_linenumber, 5) = 0, null,
          map(array[1, 2, 3], array[l_orderkey, l_partkey, l_suppkey])) as l_map,
        if (mod (l_orderkey, 198000) > 99000 and mod(l_orderkey + l_linenumber, 5) = 0, null,
           array[l_orderkey, l_partkey, l_suppkey]) as l_array


from (
select  orderkey as l_orderkey,
  if (have_nulls and mod(orderkey + linenumber, 11) = 0, null, partkey) as l_partkey,
  if (have_nulls and mod(orderkey + linenumber, 13) = 0, null, suppkey) as l_suppkey,
linenumber as l_linenumber,
  if (have_nulls and mod (orderkey + linenumber, 17) = 0, null, quantity) as l_quantity,
  if (have_nulls and mod (orderkey + linenumber, 19) = 0, null, extendedprice) as l_extendedprice,
  if (have_nulls and mod (orderkey + linenumber, 23) = 0, null, shipmode) as l_shipmode,
  if (have_nulls and mod (orderkey + linenumber, 7) = 0, null, comment) as l_comment,
    if (have_nulls and mod(orderkey + linenumber, 31) = 0, null, returnflag = 'R') as is_returned,
      if (have_nulls and mod(orderkey + linenumber, 37) = 0, null, cast (quantity + 1 as real)) as l_floatQuantity
  from (select mod (orderkey, 198000) > 99000 as have_nulls, * from hive.tpch.lineitem_s where orderkey < 1000000));
  

