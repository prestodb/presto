# Iceberg Row Lineage Test Revamp

Date: 2026-08-10
Branch: `fix/row_lineage_pushdown`

## Problem

Iceberg V3 row lineage (`_row_id` / `_last_updated_sequence_number`) is tested in three places
with substantial duplication:

- `presto-iceberg/.../TestIcebergRowLineageBase.java` — an abstract class holding 4 `@Test`
  methods plus the Iceberg-API helpers.
- `presto-iceberg/.../TestIcebergRowLineage.java` — extends the base (so it inherits those 4
  tests), and adds 5 predicate-pushdown tests along with 5 private helpers that overlap the
  base's helpers.
- `presto-native-tests/.../TestIcebergV3RowLineage.java` — a standalone
  `AbstractTestQueryFramework` subclass that re-implements all 8 base helpers verbatim, and
  duplicates the base's 4 tests with cross-engine `assertQuery` calls added.

The native test is the strictly stronger form: it drives table creation and mutation through the
Iceberg API, then checks every query twice — once via `assertQuery` (native engine vs. the Java
expected query runner, over the same warehouse) and once against values derived from the Iceberg
metadata itself. The Java-only copies add maintenance cost without adding coverage.

### Verified premise: the two runners share one warehouse

`PrestoNativeQueryRunnerUtils.IcebergQueryRunnerBuilder` defaults `dataDirectory` to
`getNativeQueryRunnerParameters().dataDirectory` for both `nativeIcebergQueryRunnerBuilder()` and
`javaIcebergQueryRunnerBuilder()`. With the same `catalogType` (HADOOP) and
`addStorageFormatToPath` on both, the native runner and the expected Java runner resolve to the
same HadoopCatalog warehouse. Cross-engine `assertQuery` over an Iceberg-API-created table is
therefore genuinely comparing two engines against one set of files.

### Verified premise: the inheritance approach is idiomatic

`presto-native-tests/pom.xml` already declares the `presto-iceberg` `test-jar` as a test-scoped
dependency, and `presto-native-tests/.../TestRewriteDataFilesProcedure` already extends
`AbstractTestRewriteDataFilesProcedure` from that jar.

## Goals

1. Each row lineage scenario is defined exactly once.
2. Row lineage scenarios run cross-engine (native vs. Java) wherever cross-engine comparison is
   meaningful.
3. `presto-iceberg`'s own CI retains a lineage read-path check that needs no native build.
4. The shared Iceberg-API helpers live in one class.

## Non-goals

- No changes to production code. This is a test-only refactor.
- No new lineage scenarios beyond those that exist today.
- No unrelated refactoring of other Iceberg test classes.

## Design

### 1. `AbstractTestIcebergRowLineage` (renamed from `TestIcebergRowLineageBase`)

Location: `presto-iceberg/src/test/java/com/facebook/presto/iceberg/AbstractTestIcebergRowLineage.java`

Renamed to match the module convention (`AbstractTestRewriteDataFilesProcedure`) and to signal
that it no longer holds tests. Contains **zero `@Test` methods**; all 4 are removed.

Members:

| Group | Members |
|---|---|
| Abstract | `protected abstract File getCatalogDirectory()` |
| Constants | `TEST_SCHEMA`, `TEST_TABLE_SCHEMA` |
| Catalog | `loadCatalog()`, `createTestTable(catalog, tableId, formatVersion)` |
| Writers | `writeFile(table, writeSchema, records…) → DataFile`, `writeRecords(table, records…)`, `writeRecordsWithSchema(table, writeSchema, records…)`, `appendOneRow(table, id, value)` |
| Iceberg metadata | `buildExpectedPairs(table, firstRowIdMessage)` |
| Pure verifiers | `assertRowLineagePairs(result, expectedPairs)`, `rowIdAndSeqById(result)`, `idsOf(result)` |
| Query helpers | `readIdAndSequenceNumber(tableName)`, `sequenceNumberForId(rows, id)`, `assertIdsForPredicate(tableName, predicate, expectedIds)`, `completedSplitsFor(sql)`, `assertPrestoRowLineageMatchesExpected(tableName, expectedPairs)` |
| Cross-engine seam | `protected void assertMatchesReferenceEngine(String sql)` |

`TEST_TABLE_SCHEMA` replaces both the inline schema inside the current `createTestTable` and the
identical `PUSHDOWN_TABLE_SCHEMA` constant in `TestIcebergRowLineage`.

**Writer consolidation.** Three overlapping writer implementations exist today: the base's
`writeRecords` (write + append + commit, uses `table.schema()`), the base's
`writeRecordsWithSchema` (write + append + commit, explicit schema), and
`TestIcebergRowLineage.writeFileWithSchema` (write only, returns the `DataFile` uncommitted — the
compaction test needs the uncommitted handle to pass to `newRewrite()`). All three collapse onto a
single `writeFile(table, writeSchema, records…) → DataFile` primitive:

- `writeFile` writes a Parquet data file under `<table location>/data/` with a random UUID name,
  using `.schema(writeSchema).withSpec(table.spec()).metricsConfig(MetricsConfig.forTable(table))`,
  and returns `writer.toDataFile()` without committing.
- `writeRecords(table, records…)` = `writeFile(table, table.schema(), records…)` then
  `newAppend().appendFile(…).commit()`.
- `writeRecordsWithSchema(table, schema, records…)` = `writeFile(table, schema, records…)` then
  `newAppend().appendFile(…).commit()`.
- `appendOneRow(table, id, value)` builds a `GenericRecord` from `table.schema()`, calls
  `writeRecords`, then `table.refresh()`.

Note: the current base `writeRecords` uses `.forTable(table)` rather than the explicit
`.schema(...).withSpec(...).metricsConfig(...)` triplet. `Parquet.writeData(...).forTable(table)`
sets exactly those three from the table, so routing `writeRecords` through `writeFile` with
`table.schema()` is behavior-preserving.

**Assertion split.** `assertPrestoRowLineageMatchesExpected(tableName, expectedPairs)` currently
executes the query itself, which prevents a caller from reusing the result. It splits into:

- `assertRowLineagePairs(MaterializedResult result, List<long[]> expectedPairs)` — pure
  verification, no query execution. Asserts row count, non-null `_row_id` and
  `_last_updated_sequence_number`, and equality with each expected `(firstRowId + position,
  dataSequenceNumber)` pair.
- `assertPrestoRowLineageMatchesExpected(tableName, expectedPairs)` — builds the SQL, calls
  `assertMatchesReferenceEngine(sql)`, then `assertRowLineagePairs(computeActual(sql), pairs)`.

**Cross-engine seam.** The base's query helpers need to perform a native-vs-Java comparison when
run from `presto-native-tests` and skip it when run from `presto-iceberg` — where
`AbstractTestQueryFramework`'s default expected query runner is H2-backed and knows nothing about
these Iceberg-only tables, so `assertQuery*` would be meaningless. A single protected hook carries
that difference:

```java
/**
 * Cross-checks {@code sql} against a reference engine. No-op by default; subclasses that
 * configure an expected query runner over the same warehouse override this to compare engines.
 */
protected void assertMatchesReferenceEngine(String sql) {}
```

`TestIcebergV3RowLineage` overrides it as `assertQueryOrdered(sql)`; `TestIcebergRowLineage`
inherits the no-op. The base's `assertPrestoRowLineageMatchesExpected` and `assertIdsForPredicate`
both call it before comparing against expected values.

This keeps every shared helper usable from both subclasses, and means each query runs at most
twice (once on each engine) rather than three times — which a wrapper that called
`assertQueryOrdered(sql)` and then a separately query-executing helper would have caused.

### 2. `TestIcebergRowLineage` (presto-iceberg)

Retains `createQueryRunner()` and `getCatalogDirectory()`, now extending
`AbstractTestIcebergRowLineage`.

Retains exactly one smoke test, `testV3TableRowLineageMatchesIcebergMetadata`, so presto-iceberg's
own CI job still exercises the V3 lineage read path — the `firstRowId + position` fallback,
`_row_id` uniqueness, and increasing per-commit sequence numbers — with no native build required.
It uses the inherited helpers, including the `assertPrestoRowLineageMatchesExpected` wrapper.

Removed:

- The 5 pushdown tests (`testPredicatePushdownPreCompaction`,
  `testPredicatePushdownPostCompaction`, `testV2TableLineagePredicates`,
  `testPredicateActuallyPrunesSplits`, `testDisjointOrRangesPruneMiddleFile`) — moved to the
  native test.
- The 5 private helpers (`assertIdsForPredicate`, `readIdAndSequenceNumber`,
  `sequenceNumberForId`, `appendOneRow`, `writeFileWithSchema`) and the
  `PUSHDOWN_TABLE_SCHEMA` constant — absorbed into the base.
- `completedSplitsFor` — absorbed into the base.

### 3. `TestIcebergV3RowLineage` (presto-native-tests)

Now `extends AbstractTestIcebergRowLineage`. Retains `createQueryRunner()`,
`createExpectedQueryRunner()`, and `getCatalogDirectory()` (which becomes the `@Override` of the
base's abstract method).

Its 8 duplicated helpers are deleted in favor of the inherited ones:
`assertRowLineageMatchesIcebergMetadata`, `rowIdAndSeqById`, `buildExpectedPairs`,
`createTestTable`, `writeRecords`, `writeRecordsWithSchema`, `loadCatalog`, and the local
`TEST_SCHEMA` constant.

Its 5 existing tests are retained unchanged in intent:

1. `testV3TableRowLineageMatchesIcebergMetadata`
2. `testV3TableRowLineageWithMultipleRowsPerCommit`
3. `testRowLineageBackfilledOnV2ToV3Upgrade`
4. `testRowLineageConsistentAcrossPredicateAndProjectionOnlyQueries`
5. `testRowLineageConsistentAcrossPredicateAndProjectionOnlyQueriesWithPushdownFilter`

The class overrides the cross-engine seam once, which is what turns every inherited helper into a
two-engine check:

```java
@Override
protected void assertMatchesReferenceEngine(String sql)
{
    assertQueryOrdered(sql);
}
```

The local `assertRowLineageMatchesIcebergMetadata` helper is deleted outright: with the override in
place, the inherited `assertPrestoRowLineageMatchesExpected(tableName, expectedPairs)` already does
the cross-engine comparison followed by the Iceberg-metadata comparison. Call sites switch to the
inherited name.

The ad-hoc `assertQuery(...)` calls the native tests make on one-off SQL strings (distinct counts,
null counts, per-id sequence numbers) stay as explicit `assertQuery` calls in the test bodies.

The 5 pushdown tests are moved in, with cross-engine assertions added:

- **Result-set assertions** are covered by the seam. The base's `assertIdsForPredicate` calls
  `assertMatchesReferenceEngine(sql)` before comparing the materialized ids to the expected list,
  so every predicate case in `testPredicatePushdownPreCompaction`,
  `testPredicatePushdownPostCompaction`, `testV2TableLineagePredicates`, and
  `testDisjointOrRangesPruneMiddleFile` is checked native vs. Java as well as against the expected
  values.
- **Split-count assertions** (`completedSplitsFor`, used by `testPredicateActuallyPrunesSplits`
  and `testDisjointOrRangesPruneMiddleFile`) stay single-engine, running only against the native
  runner. Rationale: split generation and metadata-based split pruning happen on the Java
  coordinator in both configurations, so a cross-engine comparison of split counts would compare
  identical code against itself. The assertions remain relative (`splitsAll > splitsPruned`)
  rather than absolute, so they are insensitive to worker count.

`testPredicatePushdownPostCompaction` is the highest-value move: it writes explicit physical
`_row_id` / `_last_updated_sequence_number` values into a compacted file via
`newRewrite().rewriteFiles(...)`, which is exactly the native-reader path this branch fixes.

## Data flow

Each test follows the same shape:

```
Iceberg API (HadoopCatalog on the shared warehouse)
  createTestTable → writeRecords / writeRecordsWithSchema / appendOneRow → table.refresh()
      │
      ├─→ buildExpectedPairs(table)            # expected values, read from Iceberg metadata
      │        (firstRowId + position, dataSequenceNumber)
      │
      └─→ Presto SQL over the same files
               ├─→ assertQuery* : native engine result == Java engine result
               └─→ assertRowLineagePairs : result == Iceberg metadata expectation
```

## Error handling / teardown

Unchanged from today: every test wraps its body in `try { … } finally { catalog.dropTable(tableId,
true); }`. The native test's existing pattern of swallowing exceptions from `dropTable` in the
`finally` block is preserved, so a teardown failure never masks the real assertion failure.

Table names remain distinct per test method, so the two test classes (which run against separate
data directories) and the methods within a class do not collide.

## Testing

- `mvn -pl presto-iceberg test -Dtest=TestIcebergRowLineage` — the Java smoke test passes and the
  module compiles with the renamed base class.
- `mvn -pl presto-native-tests test -Dtest=TestIcebergV3RowLineage` — all 10 tests (5 existing +
  5 moved) pass against a native build.
- Confirm no remaining references to `TestIcebergRowLineageBase`.
- Confirm no scenario present today is dropped. Accounting:

  | | Today | After |
  |---|---|---|
  | Java test methods run | 9 (4 inherited from base + 5 pushdown) | 1 (smoke) |
  | Native test methods run | 5 | 10 (5 existing + 5 moved) |
  | **Total methods run** | **14** | **11** |
  | **Distinct scenarios** | **10** | **10** |

  The 10 distinct scenarios today are the 4 base scenarios (duplicated verbatim in the native
  class, hence 14 methods for 10 scenarios), the 5 Java pushdown scenarios, and the native-only
  pushdown-filter scenario. After the change all 10 survive: 4 + 5 + 1 in the native class, with
  1 of the 4 additionally retained in Java as the smoke test.

## Risks

- **Native module gates most coverage.** 10 of 11 lineage test methods now require a native
  build. Mitigated by the Java smoke test, and by `assertQuery` exercising the Java engine inside
  the native tests.
- **Moved pushdown tests are new to the native runner.** `_last_updated_sequence_number` predicate
  pushdown is coordinator-side split pruning (PR #27766), so it should behave identically, but
  these five tests have never run under a native worker. If any fails, that is a real finding
  about native behavior to report, not a reason to weaken the assertion.
