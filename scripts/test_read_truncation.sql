

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





-- queries

SELECT
    orderkey,
    linenumber,
    s1,
    s2,
    s3,
    s4
FROM hive.tpch.strings
WHERE
    s1 > 'f'
    AND s2 > '1'
    AND s3 > '1'
    AND s4 > '2';

SELECT
    orderkey,
    linenumber,
    s1.s1,
    s1.s2,
    s3.s3,
    s4.s4
FROM hive.tpch.strings2
WHERE
    s1.s1 > 'f'
    AND s1.s2 > '1'
    AND s3.s3 > '1'
    AND s3.s4 > '2';

SELECT
    orderkey,
    linenumber,
    s1.s1,
    s1.s2,
    s3.s3,
    s4.s4
FROM hive.tpch.strings2
WHERE
    s1.s1 > 'f';




