# DuckLake test fixture

This directory holds a real DuckLake catalog + Parquet data set, used by
`presto-ducklake` connector tests. It is generated, not hand-written: the
DDL in `catalog.sql` and the on-disk layout of the Parquet files under
`data/` are exactly what the DuckDB `ducklake` extension produces, so tests
exercise the connector against the real thing instead of an approximation
of it.

## Files

- `fixture.sql` — the DuckDB script that creates every scenario. This is
  the file to read/edit to understand or extend the fixture; it is checked
  in with the placeholder token `@@DATA_PATH@@` in place of an absolute
  path, substituted by the generator script below.
- `generate-fixture.sh` — regenerates `catalog.sql` and `data/` from
  `fixture.sql` against a throwaway PostgreSQL 14 server.
- `catalog.sql` — a `pg_dump --inserts` dump of the DuckLake metadata
  catalog (schema + data), stripped of `psql` meta-commands.
- `data/` — the Parquet files DuckLake wrote while running `fixture.sql`,
  laid out under relative per-schema/per-table/per-partition directories
  (`tpch/orders/...`, `part/by_month/month=3/year=2024/...`, etc).

## Regenerating

```
presto-ducklake/src/test/resources/ducklake/generate-fixture.sh
```

Requirements: the DuckDB CLI on `PATH` (with network access, so the
`ducklake`, `postgres`, `tpch`, and `json` extensions can install on first
use) and a PostgreSQL 14 server. The script gets that Postgres server one
of two ways, controlled by `PG_MODE`:

- `docker` (the default, used automatically when `docker info` succeeds):
  starts a throwaway `postgres:14` container, port `55432`, removed on
  exit via a trap. This matches the PostgreSQL version Presto's own
  `TestPostgreSqlDistributedQueries` testcontainer uses.
- `local` (automatic fallback when Docker is not usable — e.g. on a Mac
  where Docker Desktop's organization-gated sign-in blocks
  `docker pull`/`docker run` for a given user/session): starts a scratch
  PostgreSQL 14 server using the Homebrew install
  (`PG_BIN=/opt/homebrew/opt/postgresql@14/bin` by default), with its data
  directory in a fresh `mktemp -d`, stopped and deleted on exit via the
  same trap. Force this mode explicitly with `PG_MODE=local
  ./generate-fixture.sh` (e.g. on a machine where Docker is present but
  broken/unauthenticated). Both modes use the same user (`postgres`),
  database (`ducklake`), and port (`55432`), so they produce an equivalent
  `catalog.sql`.

The script is idempotent: it deletes any previous `catalog.sql`/`data/`
before regenerating, and always tears down its Postgres server (container
or local `pg_ctl` instance) on exit via a trap, success or failure. It ends
by grepping the output for the invariants tests rely on (see
"Verification" below) and fails loudly if any are missing.

This fixture was last generated with `PG_MODE=local` on macOS using
PostgreSQL 14.24 (Homebrew) and DuckDB CLI v1.5.5 with `ducklake` extension
build `d8a1881e`.

## Why the data_path gets rewritten at test time

`fixture.sql` attaches the catalog with an absolute `DATA_PATH` (wherever
this checkout happens to live when the script runs), so the single global
`data_path` row in `ducklake_metadata` in the freshly generated
`catalog.sql` is an absolute path specific to the generating machine — it
is meaningless to anyone else who checks out the repo.

Everything else DuckLake records is relative to that `data_path`: every
`path` column in `ducklake_schema`, `ducklake_table`, `ducklake_data_file`,
and `ducklake_delete_file` is relative (`path_is_relative = true`), which
is the default when `DATA_PATH` is supplied at `ATTACH` time. That's what
makes the fixture portable: connector tests load `catalog.sql` into a
Testcontainers PostgreSQL and then run one `UPDATE ducklake_metadata SET
value = '<absolute path to this data/ directory>' WHERE key = 'data_path'`
(or equivalent) so every relative file path in the catalog resolves
against the test's own copy of `data/`, wherever it was checked out or
extracted to. Do not hand-edit paths in `catalog.sql` for this reason —
regenerate instead.

## Verification

The generator's final step asserts (and this run confirmed):

- `catalog.sql` defines `CREATE TABLE public.ducklake_snapshot` (the core
  DuckLake catalog schema landed).
- `catalog.sql` defines at least one `CREATE TABLE
  public.ducklake_inlined_data_*` table (the `inl.small` scenario below
  exercises data inlining).
- `data/` contains Parquet files (41, ~828 KB as last generated).
- `data/` contains at least one `*-delete*.parquet` file (3, from the
  `del.*` scenarios below).

## Scenarios

Every scenario lives in its own DuckLake schema (or its own table within a
shared schema), so connector tests can target one behavior without
depending on unrelated fixture state.

| Table | Rows | Purpose |
|---|---|---|
| `tpch.nation` | 25 | TPC-H tiny, row-for-row identical to Presto's `tpch.tiny.nation` except for `n_comment`: DuckDB's `tpch` extension generates free-text `*_comment` columns from a different text stream than Presto's dbgen, so tests compare every other column and only sanity-check the comment columns |
| `tpch.region` | 5 | ditto |
| `tpch.customer` | 1,500 | ditto; `c_acctbal` cast to `DOUBLE` |
| `tpch.orders` | 15,000 | ditto; `o_totalprice` cast to `DOUBLE` |
| `types.primitives` | 2 | one column per primitive type (`BOOLEAN` … `DECIMAL(18,3)`, `TIMESTAMP_S/MS/NS`, `TIMESTAMPTZ`, `UUID`, `JSON`, `BLOB`), plus a row that is entirely `NULL` except `id` |
| `types.nested` | 3 | `LIST`, `STRUCT`, `MAP`, and a `STRUCT` containing a `LIST`; includes empty and all-`NULL` rows |
| `types.unsupported` | 1 | an `INTERVAL` column, which Presto cannot map — used to assert the connector's table-load error |
| `part.by_identity` | 50 | partitioned by a plain integer column (5 partitions, `id=0`..`id=4`) |
| `part.by_month` | 400 | partitioned by `(month(ts), year(ts))`, spanning 2024–2025 (14 partitions) |
| `del.simple` | 900 live (1000 inserted, 100 deleted) | one `DELETE` over a known range (`id BETWEEN 100 AND 199`); produces a plain 2-column delete file |
| `del.two_snapshots` | 800 live (1000 inserted, 200 deleted across 2 commits) | two disjoint-range `DELETE`s against the same data file in two commits; the first delete file is superseded/rewritten, giving a 3-column delete file with a per-row snapshot id — see below |
| `evo.table` | 4 (2 files, differing schema) | `CREATE` + insert, `ALTER TABLE ADD COLUMN score INTEGER DEFAULT 42`, `RENAME COLUMN name TO full_name`, insert again — the two on-disk files disagree on both column count and column name |
| `inl.small` | 26 (6 inlined + 20 on disk) | `data_inlining_row_limit` scoped to this table only (10); three inserts of 1/2/3 rows stay inlined in the catalog, one 20-row insert is written as a Parquet file |
| `merge.table` | 5 (1 merged file) | 5 single-row inserts (5 separate files), then `ducklake_merge_adjacent_files('dl', 'table', schema => 'merge')` compacts them into 1 file |

Note on `data_inlining_row_limit`: DuckLake's extension default is **10
rows catalog-wide** (`ducklake_default_data_inlining_row_limit`), not 0.
`fixture.sql` therefore attaches with `DATA_INLINING_ROW_LIMIT 0` to
disable inlining everywhere, and re-enables it (to 10) only for
`inl.small` via `CALL dl.set_option('data_inlining_row_limit', 10, schema
=> 'inl', table_name => 'small')`. Without this, every small insert in the
fixture (`evo.table`'s two 2-row inserts, `merge.table`'s five 1-row
inserts, `tpch.region`'s 5-row load, etc.) would have silently been
inlined into the catalog instead of written as Parquet files, which would
have broken both the `evo.table` schema-mismatch scenario and the
`merge.table` compaction scenario (nothing to merge).

Note on `ducklake_merge_adjacent_files`: the task plan text describes a
catalog-wide `CALL ducklake_merge_adjacent_files('dl')`. This fixture
scopes the call to `schema => 'merge', 'table'` instead, on purpose: a
catalog-wide merge, run after every scenario above has been created, would
also silently compact `evo.table`'s two differently-shaped files into one
(DuckLake merges cleanly across a schema-version boundary by default) —
destroying the very property that scenario exists to test. Scoping the
merge preserves the fixture's explicit design goal that each scenario be
independently targetable.

## Delete file layouts observed

Both layouts appear in this fixture; the connector must handle both:

- **2-column** (`file_path VARCHAR, pos BIGINT`) — written the first time a
  data file gets a delete file, e.g. `del/simple/*-delete.parquet` (100
  deleted positions) and the original (now superseded/orphaned, but still
  present on disk — DuckLake schedules it for deletion but only physically
  removes it on an explicit `ducklake_cleanup_old_files` call, which this
  generator does not run) `del/two_snapshots/*-delete.parquet` from the
  first `DELETE`.
- **3-column** (`file_path VARCHAR, pos BIGINT, _ducklake_internal_snapshot_id
  BIGINT`) — written when a second `DELETE` against the same underlying
  data file rewrites/consolidates the delete file, e.g.
  `del/two_snapshots`'s active delete file after the second `DELETE`. Each
  row keeps the snapshot id of the delete that produced it, so time travel
  can tell which rows were visible at an earlier snapshot.

Verified directly against the generated files:

```
$ duckdb -c "DESCRIBE SELECT * FROM 'data/del/simple/ducklake-...delete.parquet'"
file_path  VARCHAR
pos        BIGINT

$ duckdb -c "DESCRIBE SELECT * FROM 'data/del/two_snapshots/ducklake-...-5a47...-delete.parquet'"   # superseded, 1st delete
file_path  VARCHAR
pos        BIGINT

$ duckdb -c "DESCRIBE SELECT * FROM 'data/del/two_snapshots/ducklake-...-5a4d...-delete.parquet'"   # active, 2nd delete
file_path                        VARCHAR
pos                              BIGINT
_ducklake_internal_snapshot_id   BIGINT
```

`catalog.sql`'s `ducklake_delete_file` rows confirm which is active:

```
-- table_id=16 (del.simple): single delete, plain 2-column format, partial_max NULL
INSERT INTO public.ducklake_delete_file VALUES (28, 16, 21, NULL, 27, '...-5a2f...-delete.parquet', true, 'parquet', 100, ..., NULL);

-- table_id=17 (del.two_snapshots): active delete file after the rewrite, partial_max = 25
INSERT INTO public.ducklake_delete_file VALUES (31, 17, 24, NULL, 29, '...-5a4d...-delete.parquet', true, 'parquet', 200, ..., 25);
```

and `ducklake_files_scheduled_for_deletion` shows the orphaned first
delete file for `del.two_snapshots` (data_file_id 30, path `...-5a47...
-delete.parquet`), which is why it is still present on disk even though it
is no longer the active delete file.

## The merged file (`merge.table`)

`ducklake_merge_adjacent_files('dl', 'table', schema => 'merge')` compacts
the 5 single-row files into 1. The catalog records this as:

```
-- the 5 original files (data_file_id 38-42) are scheduled for deletion:
INSERT INTO public.ducklake_files_scheduled_for_deletion VALUES (38, 'merge/table/...parquet', true, ...);
... (39, 40, 41, 42 likewise)

-- the merged file (data_file_id 43) carries partial_max = 44:
INSERT INTO public.ducklake_data_file VALUES (43, 23, 40, NULL, NULL, '...-5ad9...parquet', true, 'parquet', 5, ..., partial_max=44);
```

The merged Parquet file's DuckLake-visible schema is unchanged
(`id INTEGER, val VARCHAR`), but the file itself physically embeds an
extra hidden column carrying each row's original snapshot id — visible via
`read_parquet()` or `parquet_schema()`, which bypass DuckLake's column
projection, but not via a normal `SELECT * FROM dl.merge.table`:

```
$ duckdb -c "SELECT * FROM read_parquet('data/merge/table/ducklake-...-5ad9...parquet') LIMIT 3"
┌───────┬─────────┬─────────────────────────────────┐
│  id   │   val   │  _ducklake_internal_snapshot_id  │
├───────┼─────────┼─────────────────────────────────┤
│     1 │ a       │                                40 │
│     2 │ b       │                                41 │
│     3 │ c       │                                42 │
└───────┴─────────┴─────────────────────────────────┘
```

## Snapshots / commit messages

Two snapshots carry an author + commit message (via `CALL
dl.set_commit_message(...)`), stored in `ducklake_snapshot_changes`:

| snapshot_id | author | commit_message |
|---|---|---|
| 2 | `fixture-generator` | `Load TPC-H tiny fixture (nation, region, customer, orders)` |
| 20 | `fixture-generator` | `Insert 1000 rows for del.simple fixture` |

Other snapshot ids worth knowing when writing tests against this exact
generated fixture (these will differ across regenerations — always derive
them from `ducklake_snapshot`/`ducklake_column`/`ducklake_data_file`,
don't hardcode them in test assertions):

- `evo.table` created at snapshot 27; `ADD COLUMN score` at snapshot 29;
  `RENAME COLUMN name TO full_name` at snapshot 30 (the old `name` column
  row in `ducklake_column` has `end_snapshot = 30`, the new `full_name`
  row has `begin_snapshot = 30`).
- `del.two_snapshots`'s active (rewritten) delete file has
  `begin_snapshot = 24`, `partial_max = 25` (i.e., it consolidates deletes
  that happened at snapshots 24 and 25).
- `merge.table`'s merged file has `begin_snapshot = 40`, `partial_max = 44`
  (i.e., it consolidates data originally written across snapshots 40–44).

## Regeneration environment note

`fixture.sql` is intentionally *not* meant to be run directly with `duckdb
< fixture.sql` — it contains the `@@DATA_PATH@@` placeholder, substituted
by `generate-fixture.sh` with an absolute path before invoking the CLI.
Reference syntax for every DuckLake feature used here (`ATTACH
'ducklake:postgres:...'`, `set_option(...)`, `set_commit_message(...)`,
`ducklake_merge_adjacent_files(...)`, partition transforms) was taken from
the DuckLake extension's own SQL test suite
(`ducklake/test/sql/{attach,data_inlining,compaction,partitioning,audit}/*.test`)
to guarantee it matches what the extension actually accepts.
