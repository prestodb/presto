# Multi-Filter Gate — Follow-ups

Tracking remaining work after the multi-filter gate fix landed on `dynmfil-p1`.

## Context

The original DPP regression on TPC-DS Q48/Q13/Q17/Q23/Q25/Q29 was caused by
the Iceberg scan's all-or-nothing wait gate: WAITING_FOR_FILTER and
WARMUP_PAUSED both required *all* relevant dynamic filters to be complete
before exiting. With one slow build (e.g., 9.5M-row `customer_address`),
the scan blocked the full `distributed_dynamic_filter_max_wait_time` (300 s
in the cluster config) waiting on the slow filter, then dispatched all
splits at once unfiltered.

The fix swaps the gate to "first useful constraint" and adds per-batch
re-narrowing so late-arriving filters still tighten subsequent batches.

## Done

| Item | Commit |
|------|--------|
| `DynamicFilter.hasAnyComplete` SPI method (+ EMPTY override, javadoc) | f22562eb069 |
| `TableScanDynamicFilter.hasAnyComplete` impl | f22562eb069 |
| `IcebergSplitSource` gate semantic (`shouldExitWaiting`), partial-predicate manifest pruning, per-batch narrowing in `SCANNING_FILTERED`, close-time predicate catch-up, close-time emission of `DYNAMIC_FILTER_PUSHED_INTO_SCAN` and `DYNAMIC_FILTER_CONSTRAINT_COLUMNS` (single-shot under per-batch narrowing) | f22562eb069 |
| `dynamicFilterApplied` broadened to "any filter completed" so `splitsBeforeFilter` accounting is correct under partial-completion gate exits | f22562eb069 |
| `TestTableScanDynamicFilter` — 4 unit test cases for `hasAnyComplete` | f22562eb069 |
| `testMultiFilterFastUnblocksScan` — multi-filter integration test analogous to Q48 (small dim + 90K-row dim) | 76515f7b51f |

## Integration test coverage

`AbstractTestDynamicPartitionPruning` integration suite: **332 / 337
invocations pass** (33 distinct test methods × invocationCount).

The remaining 5 are all `testPartitionedMultiJoinNoFilterTimeoutWithSpill`,
failing with `spill_enabled cannot be set to true; no spill paths configured`
— a test-environment issue (cluster needs `spill.spiller-spill-path`),
pre-existing, unrelated to this change. Verified by running on the
unchanged baseline: same failure mode.

**What's exercised by the existing + new tests:**

| Scenario | Covered by |
|---|---|
| Single filter, fast resolution | `testDynamicPartitionPruningMetrics`, `testDynamicPartitionPruningResultCorrectness` |
| Single filter, hard timeout (1 ms) | `testDynamicPartitionPruningTimeout` |
| Single filter, non-selective (resolves to `all()`) | `testNonSelectiveFilterNoPruning` |
| Multi-filter, both resolve fast | `testStarSchemaMultipleFilters` |
| Multi-filter, progressive completion + multi-column constraint | `testStarSchemaProgressiveRefinement` |
| Multi-filter, mixed selective + short-circuit (`all()`) | `testStarSchemaShortCircuitWithMixedFilters` |
| Multi-filter, multi-key on same join (composite filter) | `testMultipleEquiJoinKeysDynamicPartitionPruning` |
| Multi-filter, fast small dim + slow 90K-row dim (Q48 shape) | `testMultiFilterFastUnblocksScan` (new) |
| Year-transform partition + filter | `testDynamicPartitionPruningWithYearTransform` |
| Broadcast join distribution | `testBroadcastJoinDynamicPartitionPruning` |
| Warmup state machine (manifest re-scan vs inline) | `testWarmupScanBehavior` |

## Remaining

### Runtime "drop slow filter" (deferred, conditional)

Only do this if the TPC-DS validation (owned in a separate thread) reveals
a query where Phase 1 alone doesn't help — specifically, a *single*-filter
scan whose one filter is slow. Phase 1 helps when at least one of multiple
filters is fast; it doesn't help when the only filter is the slow one.

**Sketch (do not implement yet):**
- In `IcebergSplitSource.WAITING_FOR_FILTER`, if no filter has produced any
  partition data within a short bound (≪ wait timeout, e.g., 1–2 s), drop
  the filter from the scan's wait list and dispatch unfiltered.
- Mirrors Impala's "dynamic disabling of ineffective filters" but
  triggered on lateness rather than selectivity.
- Single-filter slow-build scans become "no DPP" instead of "wait full
  timeout then no DPP".

Skip if all six TPC-DS queries clear with the current fix.

## Order of operations

1. (Owned elsewhere) TPC-DS validation against the regression set.
2. If a single-filter regression remains, implement runtime drop and
   add a corresponding integration test (e.g., a query with one filter
   on `dim_high_cardinality` and a deliberately tight wait timeout).
3. Otherwise, this file is done.
