-- DuckLake test fixture generator.
--
-- This file is NOT meant to be run directly with `duckdb < fixture.sql`: the
-- ATTACH statement below contains the placeholder token `@@DATA_PATH@@`,
-- which generate-fixture.sh substitutes with an absolute path to the local
-- `data/` directory before feeding the result to the DuckDB CLI. See
-- generate-fixture.sh and README.md for how (and why) to regenerate.
--
-- Every scenario lives in its own DuckLake schema (or its own table within a
-- shared schema) so that connector tests can target exactly one behavior
-- without pulling in unrelated fixture state.

INSTALL ducklake;
LOAD ducklake;
INSTALL postgres;
LOAD postgres;
INSTALL tpch;
LOAD tpch;
INSTALL json;
LOAD json;

-- DuckLake defaults data_inlining_row_limit to 10 rows *catalog-wide*, which
-- would silently inline most of the small inserts below instead of writing
-- them as Parquet files. Disable inlining at the catalog level and turn it
-- back on only for the inl.small scenario, which is the one meant to
-- exercise it (see CALL dl.set_option(...) below).
ATTACH 'ducklake:postgres:dbname=ducklake host=127.0.0.1 port=55432 user=postgres password=ducklake' AS dl (DATA_PATH '@@DATA_PATH@@/', DATA_INLINING_ROW_LIMIT 0);

-----------------------------------------------------------------------------
-- tpch: nation, region, customer, orders at scale factor 0.01, which is the
-- same row counts as Presto's tpch.tiny (25 nations, 5 regions, 1500
-- customers, 15000 orders). Decimal price columns are cast to DOUBLE so
-- results can be compared directly against the Presto tpch connector.
--
-- dbgen() populates tables in the current (default, in-memory) database, so
-- this section deliberately runs before `USE dl` below.
-----------------------------------------------------------------------------
CALL dbgen(sf = 0.01);

CREATE SCHEMA dl.tpch;

BEGIN;

CREATE TABLE dl.tpch.nation AS SELECT * FROM nation;

CREATE TABLE dl.tpch.region AS SELECT * FROM region;

CREATE TABLE dl.tpch.customer AS
    SELECT c_custkey, c_name, c_address, c_nationkey, c_phone,
           CAST(c_acctbal AS DOUBLE) AS c_acctbal, c_mktsegment, c_comment
    FROM customer;

CREATE TABLE dl.tpch.orders AS
    SELECT o_orderkey, o_custkey, o_orderstatus,
           CAST(o_totalprice AS DOUBLE) AS o_totalprice,
           o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment
    FROM orders;

CALL dl.set_commit_message('fixture-generator', 'Load TPC-H tiny fixture (nation, region, customer, orders)');

COMMIT;

USE dl;

-----------------------------------------------------------------------------
-- types: one table per type-coverage scenario.
-----------------------------------------------------------------------------
CREATE SCHEMA types;

-- types.primitives: one column per supported primitive type, plus a row that
-- is entirely NULL (other than the row id) so tests can exercise null
-- handling for every type.
CREATE TABLE types.primitives (
    id BIGINT,
    bool_col BOOLEAN,
    tinyint_col TINYINT,
    smallint_col SMALLINT,
    int_col INTEGER,
    bigint_col BIGINT,
    float_col FLOAT,
    double_col DOUBLE,
    decimal_col DECIMAL(18,3),
    varchar_col VARCHAR,
    date_col DATE,
    time_col TIME,
    timestamp_col TIMESTAMP,
    timestamp_s_col TIMESTAMP_S,
    timestamp_ms_col TIMESTAMP_MS,
    timestamp_ns_col TIMESTAMP_NS,
    timestamptz_col TIMESTAMPTZ,
    uuid_col UUID,
    json_col JSON,
    blob_col BLOB
);

INSERT INTO types.primitives VALUES (
    1,
    true,
    12,
    1234,
    123456,
    1234567890123,
    3.14,
    2.718281828,
    12345.678,
    'hello world',
    DATE '2024-06-15',
    TIME '13:45:30',
    TIMESTAMP '2024-06-15 13:45:30.123456',
    TIMESTAMP_S '2024-06-15 13:45:30',
    TIMESTAMP_MS '2024-06-15 13:45:30.123',
    TIMESTAMP_NS '2024-06-15 13:45:30.123456789',
    TIMESTAMPTZ '2024-06-15 13:45:30.123456+00',
    '550e8400-e29b-41d4-a716-446655440000'::UUID,
    '{"key": "value", "n": 1}'::JSON,
    'Hello World'::BLOB
);

-- the NULL row: only the id column is populated.
INSERT INTO types.primitives (id) VALUES (2);

-- types.nested: list, struct, map, and a struct containing a list.
CREATE TABLE types.nested (
    id INTEGER,
    list_col INTEGER[],
    struct_col STRUCT(a INTEGER, b VARCHAR),
    map_col MAP(VARCHAR, INTEGER),
    struct_with_list STRUCT(name VARCHAR, tags VARCHAR[])
);

INSERT INTO types.nested VALUES
    (1, [1, 2, 3], {'a': 10, 'b': 'ten'}, MAP {'x': 1, 'y': 2}, {'name': 'first', 'tags': ['a', 'b']}),
    (2, [], {'a': NULL, 'b': NULL}, MAP {}, {'name': 'second', 'tags': []}),
    (3, NULL, NULL, NULL, NULL);

-- types.unsupported: an INTERVAL column, which the Presto connector cannot
-- map to a Presto type. Tests assert that loading this table produces the
-- expected "unsupported type" error.
CREATE TABLE types.unsupported (
    id INTEGER,
    interval_col INTERVAL
);

INSERT INTO types.unsupported VALUES (1, INTERVAL '1 year 2 months 3 days');

-----------------------------------------------------------------------------
-- part: partitioning scenarios.
-----------------------------------------------------------------------------
CREATE SCHEMA part;

-- part.by_identity: partitioned by a plain integer column.
CREATE TABLE part.by_identity (id INTEGER, val VARCHAR);

ALTER TABLE part.by_identity SET PARTITIONED BY (id);

INSERT INTO part.by_identity SELECT i % 5, 'row_' || i FROM range(50) t(i);

-- part.by_month: partitioned by month(ts) and year(ts) transforms so the
-- data spans multiple partitions across a year boundary.
CREATE TABLE part.by_month (id INTEGER, ts TIMESTAMP, val VARCHAR);

ALTER TABLE part.by_month SET PARTITIONED BY (month(ts), year(ts));

INSERT INTO part.by_month
    SELECT i, TIMESTAMP '2024-01-01 00:00:00' + INTERVAL (i) DAY, 'row_' || i
    FROM range(400) t(i);

-----------------------------------------------------------------------------
-- del: delete-file scenarios.
-----------------------------------------------------------------------------
CREATE SCHEMA del;

-- del.simple: 1000 rows, then one DELETE removing a known range (100..199).
CREATE TABLE del.simple (id INTEGER, val VARCHAR);

BEGIN;

INSERT INTO del.simple SELECT i, 'row_' || i FROM range(1000) t(i);

CALL dl.set_commit_message('fixture-generator', 'Insert 1000 rows for del.simple fixture');

COMMIT;

DELETE FROM del.simple WHERE id BETWEEN 100 AND 199;

-- del.two_snapshots: all rows inserted in a single commit (one data file),
-- then two separate DELETE statements (two separate commits) against
-- disjoint key ranges of that same file, producing a rewritten delete file
-- with per-row snapshot ids.
CREATE TABLE del.two_snapshots (id INTEGER, val VARCHAR);

INSERT INTO del.two_snapshots SELECT i, 'row_' || i FROM range(1000) t(i);

DELETE FROM del.two_snapshots WHERE id BETWEEN 0 AND 99;

DELETE FROM del.two_snapshots WHERE id BETWEEN 500 AND 599;

-----------------------------------------------------------------------------
-- evo: schema evolution scenario.
-----------------------------------------------------------------------------
CREATE SCHEMA evo;

-- evo.table: create + insert, ADD COLUMN with a DEFAULT, RENAME COLUMN,
-- insert again. The first two rows live in a file written under the
-- original schema (no score column); the last two rows live in a file
-- written after the ADD COLUMN + RENAME COLUMN, so the two files disagree
-- on both column count and column name.
CREATE TABLE evo.table (id INTEGER, name VARCHAR);

INSERT INTO evo.table VALUES (1, 'alice'), (2, 'bob');

ALTER TABLE evo.table ADD COLUMN score INTEGER DEFAULT 42;

ALTER TABLE evo.table RENAME COLUMN name TO full_name;

INSERT INTO evo.table VALUES (3, 'carol', 100), (4, 'dave', 200);

-----------------------------------------------------------------------------
-- inl: data inlining scenario.
-----------------------------------------------------------------------------
CREATE SCHEMA inl;

CREATE TABLE inl.small (id INTEGER, val VARCHAR);

-- scope data inlining to this table only, with a low row limit so both
-- inlined (small) and non-inlined (large) inserts are easy to produce.
CALL dl.set_option('data_inlining_row_limit', 10, schema => 'inl', table_name => 'small');

-- below the limit: these inserts stay inlined in the catalog and never
-- become their own Parquet files.
INSERT INTO inl.small VALUES (1, 'a');

INSERT INTO inl.small VALUES (2, 'b'), (3, 'c');

INSERT INTO inl.small VALUES (4, 'd'), (5, 'e'), (6, 'f');

-- above the limit: this insert is written directly as a Parquet file.
INSERT INTO inl.small SELECT i, 'bulk_' || i FROM range(20) t(i);

-----------------------------------------------------------------------------
-- merge: compaction scenario.
-----------------------------------------------------------------------------
CREATE SCHEMA merge;

-- merge.table: several small inserts (each its own commit, each its own
-- data file), then ducklake_merge_adjacent_files() compacts them into a
-- single file. The merge call is scoped to this table (schema => 'merge')
-- rather than run catalog-wide, so it does not also compact the evo.table
-- files above (which must keep their differing schemas) or any other
-- scenario's files.
CREATE TABLE merge.table (id INTEGER, val VARCHAR);

INSERT INTO merge.table VALUES (1, 'a');

INSERT INTO merge.table VALUES (2, 'b');

INSERT INTO merge.table VALUES (3, 'c');

INSERT INTO merge.table VALUES (4, 'd');

INSERT INTO merge.table VALUES (5, 'e');

CALL ducklake_merge_adjacent_files('dl', 'table', schema => 'merge');
