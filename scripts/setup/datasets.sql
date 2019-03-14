SET session aria_scan = FALSE;

CREATE TABLE hive.tpch.lineitem1 AS
SELECT
    *,
    map(ARRAY[1, 2, 3], ARRAY[l_orderkey, l_partkey, l_suppkey]) AS l_map,
    ARRAY[l_orderkey,
    l_partkey,
    l_suppkey] AS l_array
FROM (
    SELECT
        orderkey AS l_orderkey,
        partkey AS l_partkey,
        suppkey AS l_suppkey,
        linenumber AS l_linenumber,
        quantity AS l_quantity,
        extendedprice AS l_extendedprice,
        shipmode AS l_shipmode,
        COMMENT AS l_comment,
        returnflag = 'R' AS is_returned,
        CAST(quantity + 1 AS REAL) AS l_floatQuantity
    FROM tpch.sf1.lineitem
);

-- Alternating stretches of non-nulls for 99K rows followed by 99K rows where non-key columns have nulls every few rows.
CREATE TABLE hive.tpch.lineitem1_nulls AS
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
        FROM tpch.sf1.lineitem
    )
);
  

CREATE TABLE hive.tpch.lineitem1_struct AS
SELECT
    l.orderkey AS l_orderkey,
    linenumber AS l_linenumber,
    CAST(
        ROW (
            l.partkey,
            l.suppkey,
            extendedprice,
            discount,
            quantity,
            shipdate,
            receiptdate,
            commitdate,
            l.comment
        ) AS ROW(
            l_partkey BIGINT,
            l_suppkey BIGINT,
            l_extendedprice DOUBLE,
            l_discount DOUBLE,
            l_quantity DOUBLE,
            l_shipdate DATE,
            l_receiptdate DATE,
            l_commitdate DATE,
            l_comment VARCHAR(44)
        )
    ) AS l_shipment,
    CASE
        WHEN S.nationkey = C.nationkey THEN NULL
        ELSE CAST(
            ROW(
                S.NATIONKEY,
                C.NATIONKEY,
                CASE
                    WHEN (S.NATIONKEY IN (6, 7, 19) AND C.NATIONKEY IN (6, 7, 19)) THEN 1
                    ELSE 0
                END,
                CASE
                    WHEN s.nationkey = 24 AND c.nationkey = 10 THEN 1
                    ELSE 0
                END,
                CASE
                    WHEN p.comment LIKE '%fur%' OR p.comment LIKE '%care%' THEN ROW(
                        o.orderdate,
                        l.shipdate,
                        l.partkey + l.suppkey,
                        CONCAT(p.comment, l.comment)
                    )
                    ELSE NULL
                END
            ) AS ROW (
                s_nation BIGINT,
                c_nation BIGINT,
                is_inside_eu int,
                is_restricted int,
                license ROW (applydate DATE, grantdate DATE, filing_no BIGINT, COMMENT VARCHAR)
            )
        )
    END AS l_export
FROM tpch.sf1.lineitem l,
    tpch.sf1.orders o,
    tpch.sf1.customer c,
    tpch.sf1.supplier s,
    tpch.sf1.part p
WHERE
    l.orderkey = o.orderkey
    AND l.partkey = p.partkey
    AND l.suppkey = s.suppkey
    AND c.custkey = o.custkey;


CREATE TABLE hive.tpch.cust_order_line1 AS
SELECT
    c_custkey,
    MAX(c_name) AS c_name,
    MAX(c_address) AS c_address,
    MAX(c_nationkey) AS c_nationkey,
    MAX(c_phone) AS c_phone,
    MAX(c_acctbal) AS c_acctbal,
    MAX(c_mktsegment) AS c_mktsegment,
    MAX(c_comment) AS c_comment,
    ARRAY_AGG(
        CAST(
            ROW (
                o_orderkey,
                o_orderstatus,
                o_totalprice,
                o_orderdate,
                o_orderpriority,
                o_shippriority,
                o_clerk,
                o_comment,
                LINES
            ) AS ROW(
                o_orderkey BIGINT,
                o_orderstatus VARCHAR,
                o_totalprice DOUBLE,
                o_orderdate DATE,
                o_orderpriority VARCHAR,
                o_shippriority VARCHAR,
                o_clerk VARCHAR,
                o_comment VARCHAR,
                o_lines ARRAY (
                    ROW (
                        l_partkey BIGINT,
                        l_suppkey BIGINT,
                        l_linenumber INTEGER,
                        l_quantity DOUBLE,
                        l_extendedprice DOUBLE,
                        l_discount DOUBLE,
                        l_tax DOUBLE,
                        l_returnflag VARCHAR(1),
                        l_linestatus VARCHAR(1),
                        l_shipdate DATE,
                        l_commitdate DATE,
                        l_receiptdate DATE,
                        l_shipinstruct VARCHAR(25),
                        l_shipmode VARCHAR(10),
                        l_comment VARCHAR(44)
                    )
                )
            )
        )
    ) AS c_orders
FROM (
    SELECT
        c_custkey AS c_custkey,
        o_orderkey,
        MAX(c_name) AS c_name,
        MAX(c_address) AS c_address,
        MAX(c_nationkey) AS c_nationkey,
        MAX(c_phone) AS c_phone,
        MAX(c_acctbal) AS c_acctbal,
        MAX(c_mktsegment) AS c_mktsegment,
        MAX(c_comment) AS c_comment,
        MAX(o_orderstatus) AS o_orderstatus,
        MAX(o_totalprice) AS o_totalprice,
        MAX(o_orderdate) AS o_orderdate,
        MAX(o_orderpriority) AS o_orderpriority,
        MAX(o_clerk) AS o_clerk,
        MAX(o_shippriority) AS o_shippriority,
        MAX(o_comment) AS o_comment,
        ARRAY_AGG(
            CAST(
                ROW(
                    l.partkey,
                    l.suppkey,
                    l.linenumber,
                    l.quantity,
                    l.extendedprice,
                    l.discount,
                    l.tax,
                    l.returnflag,
                    l.linestatus,
                    l.shipdate,
                    l.commitdate,
                    l.receiptdate,
                    l.shipinstruct,
                    l.shipmode,
                    l.comment
                ) AS ROW (
                    l_partkey BIGINT,
                    l_suppkey BIGINT,
                    l_linenumber INTEGER,
                    l_quantity DOUBLE,
                    l_extendedprice DOUBLE,
                    l_discount DOUBLE,
                    l_tax DOUBLE,
                    l_returnflag VARCHAR(1),
                    l_linestatus VARCHAR(1),
                    l_shipdate DATE,
                    l_commitdate DATE,
                    l_receiptdate DATE,
                    sl_hipinstruct VARCHAR(25),
                    l_shipmode VARCHAR(10),
                    l_comment VARCHAR(44)
                )
            )
        ) AS LINES
    FROM tpch.sf1.lineitem l,
        (
        SELECT
            c.custkey AS c_custkey,
            name AS c_name,
            address AS c_address,
            nationkey AS c_nationkey,
            phone AS c_phone,
            acctbal AS c_acctbal,
            mktsegment AS c_mktsegment,
            c.comment AS c_comment,
            orderkey AS o_orderkey,
            orderstatus AS o_orderstatus,
            totalprice AS o_totalprice,
            orderdate AS o_orderdate,
            orderpriority AS o_orderpriority,
            clerk AS o_clerk,
            shippriority AS o_shippriority,
            o.comment AS o_comment
        FROM tpch.sf1.orders o,
            tpch.sf1.customer c
        WHERE
            o.custkey = c.custkey
            AND c.custkey BETWEEN 0 AND 2000000
    )
    WHERE
        o_orderkey = l.orderkey
    GROUP BY
        c_custkey,
        o_orderkey
)
GROUP BY
    c_custkey;

-- Strings
CREATE TABLE hive.tpch.strings AS
SELECT
    orderkey,
    linenumber,
    COMMENT AS s1,
    CONCAT(CAST(partkey AS VARCHAR), COMMENT) AS s2,
    CONCAT(CAST(suppkey AS VARCHAR), COMMENT) AS s3,
    CONCAT(CAST(quantity AS VARCHAR), COMMENT) AS s4
FROM tpch.sf1.lineitem
WHERE
    orderkey < 100000;

CREATE TABLE hive.tpch.strings_struct AS
SELECT
    orderkey,
    linenumber,
    CAST(
        ROW(COMMENT, CONCAT(CAST(partkey AS VARCHAR), COMMENT)) AS ROW(s1 VARCHAR, s2 VARCHAR)
    ) AS s1,
    CAST(
        ROW(
            CONCAT(CAST(suppkey AS VARCHAR), COMMENT),
            CONCAT(CAST(quantity AS VARCHAR), COMMENT)
        ) AS ROW(s3 VARCHAR, s4 VARCHAR)
    ) AS s3
FROM tpch.sf1.lineitem
WHERE
    orderkey < 100000;

CREATE TABLE hive.tpch.strings_struct_nulls AS
SELECT
    orderkey,
    linenumber,
    CAST(
        IF (
            mod(partkey, 5) = 0,
            NULL,
            ROW(
                COMMENT,
                IF (mod(partkey, 13) = 0, NULL, CONCAT(CAST(partkey AS VARCHAR), COMMENT))
            )
        ) AS ROW(s1 VARCHAR, s2 VARCHAR)
    ) AS s1,
    CAST(
        IF (
            mod (partkey, 7) = 0,
            NULL,
            ROW(
                IF (mod(suppkey, 17) = 0, NULL, CONCAT(CAST(suppkey AS VARCHAR), COMMENT)),
                CONCAT(CAST(quantity AS VARCHAR), COMMENT)
            )
        ) AS ROW(s3 VARCHAR, s4 VARCHAR)
    ) AS s3
FROM hive.tpch.lineitem_s
WHERE
    orderkey < 100000;

