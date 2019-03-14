--

-- Alternating stretches of non-nulls for 99K rows followed by 99K rows where non-key columns have nulls every few rows.

CREATE TABLE hive.tpch.lineitem_nulls AS
SELECT
    *,
    IF (
        mod (l_orderkey, 198000) > 99000
        AND mod(l_orderkey + l_linenumber, 5) = 0,
        NULL,
        map(ARRAY[1, 2, 3], ARRAY[l_orderkey, l_partkey, l_suppkey])
    ) AS l_map,
    IF (
        mod (l_orderkey, 198000) > 99000
        AND mod(l_orderkey + l_linenumber, 5) = 0,
        NULL,
        ARRAY[l_orderkey,
        l_partkey,
        l_suppkey]
    ) AS l_array
FROM (
    SELECT
        orderkey AS l_orderkey,
        IF (have_nulls
        AND mod(orderkey + linenumber, 11) = 0, NULL, partkey) AS l_partkey,
        IF (have_nulls
        AND mod(orderkey + linenumber, 13) = 0, NULL, suppkey) AS l_suppkey,
        linenumber AS l_linenumber,
        IF (have_nulls
        AND mod (orderkey + linenumber, 17) = 0, NULL, quantity) AS l_quantity,
        IF (have_nulls
        AND mod (orderkey + linenumber, 19) = 0, NULL, extendedprice) AS l_extendedprice,
        IF (have_nulls
        AND mod (orderkey + linenumber, 23) = 0, NULL, shipmode) AS l_shipmode,
        IF (have_nulls
        AND mod (orderkey + linenumber, 7) = 0, NULL, COMMENT) AS l_comment,
        IF (have_nulls
        AND mod(orderkey + linenumber, 31) = 0, NULL, returnflag = 'R') AS is_returned,
        IF (have_nulls
        AND mod(orderkey + linenumber, 37) = 0, NULL, CAST(quantity + 1 AS REAL)) AS l_floatQuantity
    FROM (
        SELECT
            mod (orderkey, 198000) > 99000 AS have_nulls,
            *
        FROM hive.tpch.lineitem_s
        WHERE
            orderkey < 1000000
    )
);
  

