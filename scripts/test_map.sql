


-- Column index of numeric/date columns to column value as long.
CREATE TABLE hive.tpch.lineitem_map AS
SELECT
    orderkey AS l_orderkey,
    linenumber AS l_linenumber,
    map(
        ARRAY[2,
        3,
        5,
        6,
        7,
        8,
        11,
        12,
        13,
        IF(receiptdate > commitdate, 100, 101),
        IF (returnflag = 'R', 102, 103),
        104],
        ARRAY[partkey,
        suppkey,
        CAST(quantity AS BIGINT),
        CAST(extendedprice AS BIGINT),
        CAST(discount * 100 AS BIGINT),
        CAST(tax * 100 AS BIGINT),
        DATE_DIFF('day', CAST('1970-1-1' AS DATE), shipdate),
        DATE_DIFF('day', CAST('1970-1-1' AS DATE), commitdate),
        DATE_DIFF('day', CAST('1970-1-1' AS DATE), receiptdate),
        DATE_DIFF('day', commitdate, receiptdate),
        CAST(extendedprice AS BIGINT),
        IF (extendedprice > 5000, CAST(extendedprice * discount AS BIGINT), NULL)]
    ) AS ints,
    map(
        ARRAY['9',
        '10',
        '13',
        '14',
        '15'],
        ARRAY[returnflag,
        linestatus,
        shipmode,
        shipinstruct,
        COMMENT]
    ) AS strs
FROM hive.tpch.lineitem;



-- 1.  orderkey      
-- 2.  partkey       
-- 3.  suppkey       
-- 4.  linenumber    
-- 5. quantity      
-- 6. extendedprice 
-- 7. discount      
-- 8.  tax           
-- 9.  returnflag    
-- 10.  linestatus    
-- 11.  shipdate      
-- 12.  commitdate    
-- 13.  receiptdate   
-- 14.  shipinstruct  
-- 15.  shipmode      
-- 16.  comment       
