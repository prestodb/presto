# Project Plan: Unified Dynamic Filtering with Coordinator-to-Worker Broadcast

## Goal

Replace the two mutually exclusive dynamic filtering modes (built-in DF for row-level
filtering, distributed DPP for split-level pruning) with a single unified mechanism that
provides both split-level pruning AND row-level filtering. This eliminates the cycle
detection stopgap in `RemoveUnsupportedDynamicFilters` and aligns the architecture with
Trino's unified approach.

## Current Architecture

### Built-in DF (`enable_dynamic_filtering`)
- **Scope**: Local, same-fragment, broadcast/replicated joins only
- **Probe side**: `PredicatePushDown` creates `FilterNode` with `$$dynamic_filter_placeholder`
  expression directly above the `TableScan`
- **Build side**: `LookupJoinOperator` collects build data, produces domain
- **Filtering**: Row-level via `FilterOperator` evaluating the placeholder expression
- **Split pruning**: None

### Distributed DPP (`distributed_dynamic_filter_enabled`)
- **Scope**: Cross-fragment, any join distribution
- **Probe side**: No `FilterNode`; `JoinNode.dynamicFilters` preserved by bypassing the
  consumption check in `RemoveUnsupportedDynamicFilters`
- **Build side**: `DynamicFilterSourceOperator` collects build data →
  `DynamicFilterFetcher` delivers to coordinator's `JoinDynamicFilter`
- **Filtering**: Split-level only via `IcebergSplitSource` using `DynamicFilter` SPI
- **Split pruning**: Yes
- **Cycle detection**: `probeChainCrossesRemoteExchange` removes filters whose probe
  chain creates a circular dependency (Q64 pattern)

### Problems with Current Architecture
1. Two mutually exclusive modes — can't have both split pruning and row filtering
2. DPP bypass in `RemoveUnsupportedDynamicFilters` requires explicit cycle detection
3. For cyclic topologies (Q64), filters are removed entirely — no filtering at all

## Target Architecture

A single mode that combines the best of both:

- **Probe side**: `FilterNode` with dynamic filter expression (like built-in DF)
- **Build side**: `DynamicFilterSourceOperator` → coordinator collection (like DPP)
- **Split-level pruning**: Coordinator applies resolved domain to split source (like DPP)
- **Row-level filtering**: Coordinator broadcasts domain to probe-side workers, which
  apply it via `FilterOperator` (new)
- **Cycle handling**: Split source uses non-blocking progressive filtering for
  cross-fragment filters — no optimizer-level cycle detection needed

### How Cycles Are Eliminated

The split source stops blocking on the `DynamicFilter` for cross-fragment filters.
Instead, it proceeds immediately and applies the filter progressively as domains arrive
via `getCurrentPredicate()`. This breaks the Q64 cycle:

1. Split source returns splits immediately (no blocking)
2. Probe pipeline starts flowing data
3. Build pipeline completes, filter domain collected by coordinator
4. Coordinator broadcasts domain to probe-side workers
5. Workers apply domain via `FilterOperator` — rows are filtered even though splits
   weren't pruned

For same-fragment filters (broadcast/replicated joins), the split source CAN block
because the build pipeline resolves independently (no cycle risk). This preserves the
existing split-pruning benefit for star schema queries.

The `RemoveUnsupportedDynamicFilters` consumption check works naturally:
- Same-fragment `FilterNode → TableScan` is consumed → filter preserved
- Cross-fragment `FilterNode → TableScan` is also consumed (traversal passes through
  `ExchangeNode`) → filter preserved → coordinator registers it → broadcast works
- The cycle is handled at the split source level (non-blocking), not the optimizer level

## Implementation Plan

### Phase 1: Probe-Side FilterNode for Distributed DF

**Goal**: Have `AddDynamicFilterRule` also create a probe-side `FilterNode` with dynamic
filter expressions, so both modes produce the same plan structure.

**Files**:
- `presto-main-base/.../iterative/rule/AddDynamicFilterRule.java`
- `presto-main-base/.../iterative/rule/AddDynamicFilterToSemiJoinRule.java`

**Changes**:

1. In `AddDynamicFilterRule.apply()`, after creating `JoinNode.dynamicFilters`, also
   create a `FilterNode` on the probe side with `createDynamicFilterExpression()` for
   each equi-join clause:

   ```java
   // Current: only creates JoinNode.dynamicFilters
   for (EquiJoinClause clause : node.getCriteria()) {
       String filterId = context.getIdAllocator().getNextId().toString();
       dynamicFilters.put(filterId, clause.getRight());
   }

   // New: also create probe-side FilterNode
   List<RowExpression> filterExpressions = new ArrayList<>();
   for (EquiJoinClause clause : node.getCriteria()) {
       String filterId = context.getIdAllocator().getNextId().toString();
       dynamicFilters.put(filterId, clause.getRight());
       filterExpressions.add(createDynamicFilterExpression(
               filterId, clause.getLeft(), functionAndTypeManager));
   }
   PlanNode newProbe = filterExpressions.isEmpty() ? node.getLeft() :
       new FilterNode(node.getLeft().getSourceLocation(),
           context.getIdAllocator().getNextId(),
           node.getLeft(),
           combineConjuncts(filterExpressions));
   ```

2. Same pattern for `AddDynamicFilterToSemiJoinRule`.

3. Remove the DPP bypass in `RemoveUnsupportedDynamicFilters.extractDynamicFilterFromJoin()`:
   the standard consumption check now works because the probe-side `FilterNode` exists.

4. Remove `probeChainCrossesRemoteExchange()` and `hasRemoteExchangeInProbeChain()`.

5. Remove the mutually exclusive validation in `SystemSessionProperties` — both modes
   can coexist.

**Testing**: Unit tests in `TestRemoveUnsupportedDynamicFilters` — verify that:
- Same-fragment filters are preserved (consumption check passes)
- Cross-fragment filters are also preserved (consumption traverses through ExchangeNode)
- All existing DPP tests still pass

**Estimated effort**: 2-3 days

---

### Phase 2: Non-Blocking Split Source for Cross-Fragment Filters

**Goal**: Make the split source non-blocking for cross-fragment dynamic filters so the
Q64 cycle cannot occur. Same-fragment filters continue to block for full split pruning.

**Files**:
- `presto-spi/.../connector/DynamicFilter.java`
- `presto-main-base/.../scheduler/JoinDynamicFilter.java`
- `presto-main-base/.../scheduler/TableScanDynamicFilter.java`
- `presto-main-base/.../sql/planner/SplitSourceFactory.java`
- `presto-iceberg/.../IcebergSplitSource.java`

**Changes**:

1. Add `boolean isBlocking()` method to `DynamicFilter` SPI (default `true` for
   backward compatibility). Non-blocking filters return `false` — the split source
   should not wait for them but should apply `getCurrentPredicate()` progressively.

2. In `JoinDynamicFilter`, add a `blocking` flag set at construction time. Cross-fragment
   filters are non-blocking; same-fragment filters are blocking.

3. In `SplitSourceFactory`, when wiring `DynamicFilter` to a table scan, determine
   whether the filter is same-fragment or cross-fragment. This information is available
   from `DynamicFilterService.probeFragmentFilters` — if the filter ID was registered
   for this fragment (same-fragment), it's blocking; if it was propagated from an
   ancestor fragment (cross-fragment), it's non-blocking.

4. In `IcebergSplitSource.getNextBatch()`, check `dynamicFilter.isBlocking()`:
   - If blocking: current behavior (wait for `isComplete()` up to timeout)
   - If non-blocking: call `initializeScan()` immediately with
     `dynamicFilter.getCurrentPredicate()` (which returns `all()` initially), and
     re-check on subsequent `getNextBatch()` calls for progressive refinement

5. `TableScanDynamicFilter` delegates `isBlocking()` — blocking only if ALL wrapped
   filters are blocking.

**Testing**:
- New integration test: Q64-like topology with non-blocking filter — verify no timeout,
  results correct
- Existing DPP tests: same-fragment filters still block and prune splits
- `testDynamicPartitionPruningTimeout`: verify non-blocking filters don't timeout

**Estimated effort**: 3-4 days

---

### Phase 3: Worker-Side DynamicFilter for Broadcast Domains

**Goal**: Create a worker-side `DynamicFilter` implementation that receives broadcast
domains from the coordinator and makes them available to the `FilterOperator`.

**Files**:
- `presto-main-base/.../execution/BroadcastDynamicFilter.java` (new)
- `presto-main-base/.../operator/TaskContext.java`
- `presto-main-base/.../sql/planner/LocalExecutionPlanner.java`

**Changes**:

1. New `BroadcastDynamicFilter` class:
   ```java
   public class BroadcastDynamicFilter
   {
       // Map of filterId -> SettableFuture<Domain>
       // Resolved when coordinator broadcasts the domain
       private final Map<String, SettableFuture<Domain>> pendingFilters;

       // Called by TaskContext when broadcast domain arrives
       public void resolveDomain(String filterId, Domain domain);

       // Called by FilterOperator to get current predicate
       public TupleDomain<ColumnHandle> getCurrentPredicate();

       // Returns a future that completes when any filter resolves
       public CompletableFuture<?> isBlocked();
   }
   ```

2. In `TaskContext`, add a `BroadcastDynamicFilter` field:
   - Set during `LocalExecutionPlanner` when planning `FilterNode → TableScan` for
     cross-fragment dynamic filters
   - Provides `resolveBroadcastDomain(String filterId, Domain domain)` method called
     when the coordinator sends the domain

3. In `LocalExecutionPlanner.visitFilter()` (or the ScanFilterAndProject path), when
   the FilterNode contains dynamic filter expressions:
   - Extract the dynamic filter descriptors
   - Create a `BroadcastDynamicFilter` for cross-fragment filter IDs
   - Wire it into the `PageProcessor` / `FilterOperator` so the placeholder expression
     resolves against the broadcast domain
   - Register the filter IDs with `TaskContext` so incoming broadcasts can be routed

**Key design decision**: The `FilterOperator` evaluates `$$dynamic_filter_placeholder`
expressions. Currently in built-in DF, these resolve against the `LookupJoinOperator`'s
collected domain (same pipeline). For broadcast, they resolve against the
`BroadcastDynamicFilter`'s domain (delivered asynchronously by coordinator). The
placeholder function needs a `DynamicFilter` supplier that returns the correct
implementation depending on the mode.

**Testing**:
- Unit test for `BroadcastDynamicFilter`: resolve domain, verify `getCurrentPredicate()`
- Integration test: verify row-level filtering works with broadcast domain

**Estimated effort**: 4-5 days

---

### Phase 4: Coordinator-to-Worker Domain Broadcast

**Goal**: Implement the mechanism for the coordinator to push resolved dynamic filter
domains to probe-side worker tasks.

**Files**:
- `presto-main-base/.../server/TaskUpdateRequest.java`
- `presto-main-base/.../execution/SqlTask.java`
- `presto-main-base/.../execution/SqlTaskManager.java`
- `presto-main-base/.../execution/buffer/TaskResource.java` (or new endpoint)
- `presto-main/.../server/remotetask/HttpRemoteTaskWithEventLoop.java`
- `presto-main-base/.../scheduler/JoinDynamicFilter.java`
- `presto-main-base/.../scheduler/DynamicFilterService.java`

**Approach**: Piggyback on the existing task update mechanism. When the coordinator sends
a `TaskUpdateRequest` to a probe-side worker, include any newly resolved dynamic filter
domains.

**Changes**:

1. Add `Map<String, Domain> dynamicFilterDomains` field to `TaskUpdateRequest`:
   ```java
   private final Map<String, Domain> dynamicFilterDomains;  // filterId -> Domain
   ```

2. In `DynamicFilterService`, maintain a mapping of which probe-side tasks need which
   filter IDs. When `JoinDynamicFilter` resolves a domain:
   - Look up all probe-side tasks that consume this filter ID
   - Queue the domain for delivery on the next task update

3. In `HttpRemoteTaskWithEventLoop.sendUpdate()`, include pending dynamic filter domains
   in the `TaskUpdateRequest`.

4. On the worker side, in `SqlTaskManager.updateTask()`, extract the
   `dynamicFilterDomains` and deliver them to the `SqlTask`:
   ```java
   if (!dynamicFilterDomains.isEmpty()) {
       sqlTask.receiveBroadcastDomains(dynamicFilterDomains);
   }
   ```

5. `SqlTask.receiveBroadcastDomains()` delegates to `TaskContext`, which resolves the
   `BroadcastDynamicFilter` futures.

6. Register probe-side tasks with `DynamicFilterService` when tasks are created:
   - `SectionExecutionFactory` already knows which filter IDs are consumed by each
     fragment (via `DynamicFilterService.probeFragmentFilters`)
   - When creating a probe-side task, register (taskId, filterIds) with the service
   - When a domain resolves, the service queues it for all registered tasks

**Alternative approach**: Instead of piggybacking on task updates, add a dedicated
`POST /v1/task/{taskId}/dynamicfilters` endpoint that the coordinator calls when a domain
resolves. This is simpler (no coupling to the task update cycle) but adds a new HTTP
endpoint.

**Recommendation**: Start with the task update piggyback approach. The coordinator
already sends frequent task updates (with new splits), so domain delivery latency is
low. If latency is a problem, switch to the dedicated endpoint later.

**Testing**:
- Unit test: verify domains are included in `TaskUpdateRequest` and delivered to task
- Integration test: end-to-end broadcast — build side collects domain, coordinator
  broadcasts to probe-side workers, workers apply row-level filtering
- Verify that broadcast happens even for non-blocking (cross-fragment) filters

**Estimated effort**: 5-6 days

---

### Phase 5: SplitSourceFactory Unification

**Goal**: Change `SplitSourceFactory` to extract dynamic filter descriptors from
`FilterNode` predicates (like Trino) instead of traversing `JoinNode.getDynamicFilters()`.

**Files**:
- `presto-main-base/.../sql/planner/SplitSourceFactory.java`

**Changes**:

1. In `visitTableScan()` / `visitFilter()`, extract dynamic filter descriptors from the
   `FilterNode` predicate (if present above the `TableScan`):
   ```java
   @Override
   public Map<PlanNodeId, SplitSource> visitFilter(FilterNode node)
   {
       if (node.getSource() instanceof TableScanNode) {
           List<DynamicFilterDescriptor> descriptors =
               extractDynamicFilters(node.getPredicate());
           DynamicFilter dynamicFilter = descriptors.isEmpty()
               ? DynamicFilter.EMPTY
               : dynamicFilterService.createDynamicFilter(queryId, descriptors, assignments);
           return createSplitSource(tableScan, dynamicFilter);
       }
       return node.getSource().accept(this, context);
   }
   ```

2. Remove the existing `registerDynamicFilters()` method that traverses
   `JoinNode.getDynamicFilters()` and matches by column name. This probe-chain traversal
   and column-name matching logic is replaced by the simpler FilterNode extraction.

3. Remove `collectProbeChainRemoteSources()` and the transitive propagation logic in
   `DynamicFilterService.probeFragmentFilters` — no longer needed because the FilterNode
   in each fragment directly identifies which filters apply to which scan.

**Testing**:
- All existing DPP integration tests
- Verify that same-fragment and cross-fragment filters are both wired correctly

**Estimated effort**: 2-3 days

---

### Phase 6: Cleanup

**Goal**: Remove all DPP-specific code paths that are no longer needed.

**Files**:
- `presto-main-base/.../iterative/rule/RemoveUnsupportedDynamicFilters.java`
- `presto-main-base/.../SystemSessionProperties.java`
- `presto-main-base/.../sql/planner/sanity/DynamicFiltersChecker.java`
- Various test files

**Changes**:

1. Remove `probeChainCrossesRemoteExchange()` and `hasRemoteExchangeInProbeChain()`
   from `RemoveUnsupportedDynamicFilters`.

2. Remove the DPP bypass branch in `extractDynamicFilterFromJoin()` — the standard
   consumption check is now the only path.

3. Remove or unify the session properties:
   - Deprecate `distributed_dynamic_filter_enabled` / `distributed_dynamic_filter_strategy`
   - Unified mode is controlled by `enable_dynamic_filtering` with optional
     `dynamic_filter_max_wait_time` for split-source blocking behavior
   - Or: keep both properties but remove the mutually exclusive validation

4. Remove the `isDistributedDynamicFilterEnabled()` checks scattered through:
   - `DynamicFiltersChecker` (skip probe-side consumption verify)
   - `RemoveUnsupportedDynamicFilters` (DPP bypass)
   - `AddDynamicFilterRule` / `AddDynamicFilterToSemiJoinRule` (gating)

5. Update all test files to use the unified mode.

**Estimated effort**: 2-3 days

---

## Phase Summary

| Phase | Description | Effort | Dependencies |
|-------|-------------|--------|--------------|
| 1 | Probe-side FilterNode for distributed DF | 2-3 days | None |
| 2 | Non-blocking split source for cross-fragment filters | 3-4 days | Phase 1 |
| 3 | Worker-side BroadcastDynamicFilter | 4-5 days | Phase 1 |
| 4 | Coordinator-to-worker domain broadcast | 5-6 days | Phase 3 |
| 5 | SplitSourceFactory unification | 2-3 days | Phase 1, 4 |
| 6 | Cleanup | 2-3 days | All above |
| **Total** | | **~3-4 weeks** | |

Phases 2 and 3 are independent of each other (both depend only on Phase 1) and can be
developed in parallel.

## Key Design Decisions

### 1. Blocking vs Non-Blocking Split Source

Same-fragment filters block the split source (full split pruning, no cycle risk).
Cross-fragment filters are non-blocking (progressive filtering, cycle-safe).

The determination is made at the `SplitSourceFactory` level based on whether the filter
was registered for this fragment (same-fragment = blocking) or propagated from an
ancestor (cross-fragment = non-blocking).

### 2. Broadcast Mechanism

Piggyback on `TaskUpdateRequest` rather than adding a new HTTP endpoint. The coordinator
already sends frequent updates to workers (split assignments), so domain delivery latency
is low. Domains are queued per-task and included in the next update.

### 3. FilterNode Expression Evaluation

The `$$dynamic_filter_placeholder` expression on probe-side workers resolves against:
- **Same-fragment**: The `LookupJoinOperator`'s collected domain (existing built-in DF
  path — no change needed)
- **Cross-fragment**: The `BroadcastDynamicFilter`'s domain (new path — coordinator
  broadcasts the domain)

The `LocalExecutionPlanner` determines which path to use based on whether the
`DynamicFilterSourceOperator` exists in the same fragment (same-fragment) or not
(cross-fragment → broadcast).

### 4. Backward Compatibility

The old session properties (`distributed_dynamic_filter_enabled`,
`distributed_dynamic_filter_strategy`) continue to work during migration. The unified
mode is the default for new queries. Old properties are deprecated and eventually removed.

## Risks and Mitigations

### Risk: Non-blocking split source loses pruning for simple cross-fragment joins
**Mitigation**: For simple partitioned joins (no cycle risk), the build side typically
resolves within seconds. The non-blocking split source still applies the filter
progressively — splits enumerated after the domain arrives are pruned. Only the first
few splits (enumerated before the domain arrives) miss pruning, and those are filtered
at row level by the broadcast domain on workers. The net performance impact is small.

### Risk: Broadcast latency delays row-level filtering
**Mitigation**: The coordinator sends domains via task updates, which are frequent during
active execution. Typical broadcast latency is < 1 second. For the brief window before
the domain arrives, rows pass through unfiltered (same as not having the filter at all).

### Risk: Large domains consume memory on workers
**Mitigation**: The same domain size limits that apply to coordinator-side
`JoinDynamicFilter` apply to broadcast. If a domain exceeds the size limit, it's
converted to a range or `all()` on the coordinator side before broadcast.

### Risk: Many workers × many filters = many broadcast messages
**Mitigation**: Domains are piggybacked on existing task updates (no extra HTTP calls).
Multiple domains are batched in a single update. The coordinator only sends each domain
once per task (not repeatedly).

## Verification Plan

1. **Unit tests**: `BroadcastDynamicFilter`, non-blocking `JoinDynamicFilter`,
   `TaskUpdateRequest` serialization with domains
2. **Optimizer tests**: `TestRemoveUnsupportedDynamicFilters` — verify standard
   consumption check works for both same-fragment and cross-fragment
3. **Integration tests**: All existing `TestDynamicPartitionPruning` tests pass
4. **New integration tests**:
   - Cross-fragment filter with row-level filtering via broadcast
   - Q64-like topology: no timeout, correct results, row-level filtering applied
   - Mixed topology: same-fragment (blocking, split pruning) + cross-fragment
     (non-blocking, row-level filtering)
5. **Performance validation**: TPC-DS Q4, Q59, Q64 on Iceberg at sf1000
