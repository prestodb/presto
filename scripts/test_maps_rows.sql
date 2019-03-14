

-- custkey with map from year to an array of purchases that year.
CREATE TABLE hive.tpch.cust_year_parts AS
SELECT
    custkey,
    map_agg(y, parts) AS year_parts,
    map_agg(y, total_cost) AS year_cost
FROM (
    SELECT
        c.custkey,
        YEAR(shipdate) AS y,
        ARRAY_AGG(
            CAST(ROW (partkey, extendedprice, quantity) AS ROW (pk BIGINT, ep DOUBLE, qt DOUBLE))
        ) AS parts,
        SUM(extendedprice) AS total_cost
    FROM hive.tpch.lineitem_s l,
        hive.tpch.orders o,
        hive.tpch.customer c
    WHERE
        l.orderkey = o.orderkey
        AND o.custkey = c.custkey
        AND c.nationkey = 1
        AND quantity < 10
    GROUP BY
        c.custkey,
        YEAR(shipdate)
)
GROUP BY
    custkey;


CREATE TABLE hive.tpch.exportlineitem AS
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
FROM hive.tpch.lineitem l,
    hive.tpch.orders o,
    hive.tpch.customer c,
    hive.tpch.supplier s,
    hive.tpch.part p
WHERE
    l.orderkey = o.orderkey
    AND l.partkey = p.partkey
    AND l.suppkey = s.suppkey
    AND c.custkey = o.custkey;



SELECT
    l.applydate
FROM (
    SELECT
        e.license AS l
    FROM (
        SELECT
            export AS e
        FROM hive.tpch.exportinfo
        WHERE
            orderkey < 5
    )
);

SELECT
    orderkey,
    linenumber,
    s_nation,
    l.applydate
FROM (
    SELECT
        orderkey,
        linenumber,
        e.s_nation,
        e.license AS l
    FROM (
        SELECT
            orderkey,
            linenumber,
            export AS e
        FROM hive.tpch.exportinfo
        WHERE
            orderkey < 15
    )
);

CREATE TABLE hive.tpch.cust_order_line AS
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
    FROM hive.tpch.lineitem l,
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
        FROM hive.tpch.orders o,
            hive.tpch.customer c
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


SELECT
    COUNT(*)
FROM hive.tpch.cust_order_line
CROSS JOIN unnest (c_orders)
CROSS JOIN unnest (o_lines)
WHERE
    l_partkey = 111111;

SELECT
    COUNT(*)
FROM hive.tpch.exportlineitem
WHERE
    export.s_nation = 2;


SELECT
    COUNT(*),
    SUM(l_shipment.l_partkey)
FROM hive.tpch.exportlineitem
WHERE
    l_export.license.comment BETWEEN 'fur' AND 'hag';