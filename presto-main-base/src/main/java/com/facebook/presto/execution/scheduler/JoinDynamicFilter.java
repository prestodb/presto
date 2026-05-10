/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.execution.scheduler;

import com.facebook.airlift.units.Duration;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.spi.connector.DynamicFilter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_COLLECTION_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_COORDINATOR_FALLBACK_TO_RANGE;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_DOMAIN_RANGE_COUNT;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_EXPECTED_PARTITIONS;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PARTITIONS_RECEIVED;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_SHORT_CIRCUITED;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_TIMED_OUT;
import static com.facebook.presto.common.RuntimeUnit.NANO;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static com.facebook.presto.spi.connector.DynamicFilter.NOT_BLOCKED;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Coordinator-side per-join dynamic filter. Does NOT implement {@link DynamicFilter};
 * the SPI-facing wrapper is {@link TableScanDynamicFilter}.
 *
 * <p>Constraints are stored keyed by filter ID and translated to column names
 * at the boundary via {@link #getCurrentConstraintByColumnName()}.
 *
 * <p>Completion is gated on per-task <i>finalization</i>, not on contribution
 * count. A build-side task may emit multiple non-final contributions while the
 * hash table is still being constructed (Velox's progressive narrowing); only
 * the latest contribution per task is retained, and the merge is finalized
 * once {@link #expectedPartitions} distinct tasks have reported a final
 * contribution.
 */
@ThreadSafe
public class JoinDynamicFilter
{
    private final String filterId;
    private final String columnName;
    private final Duration waitTimeout;
    private final long maxSizeInBytes;
    private final DynamicFilterStats stats;
    private final RuntimeStats runtimeStats;
    private final boolean extendedMetrics;

    @GuardedBy("this")
    private final Map<TaskId, TupleDomain<String>> latestByTask = new HashMap<>();
    @GuardedBy("this")
    private final Set<TaskId> finalizedTasks = new HashSet<>();
    private final CompletableFuture<TupleDomain<String>> constraintByFilterIdFuture;

    private final AtomicBoolean timeoutStarted = new AtomicBoolean(false);

    @GuardedBy("this")
    private int expectedPartitions;

    private volatile boolean fullyResolved;

    @GuardedBy("this")
    private TupleDomain<String> mergedConstraint;

    @GuardedBy("this")
    private Domain probeColumnDomain;

    @GuardedBy("this")
    private long collectionStartNanos;
    @GuardedBy("this")
    private boolean collectionStarted;
    @GuardedBy("this")
    private boolean collectionTimeRecorded;

    public JoinDynamicFilter(
            String filterId,
            String columnName,
            Duration waitTimeout,
            long maxSizeInBytes,
            DynamicFilterStats stats,
            RuntimeStats runtimeStats,
            boolean extendedMetrics)
    {
        this.filterId = requireNonNull(filterId, "filterId is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.waitTimeout = requireNonNull(waitTimeout, "waitTimeout is null");
        this.maxSizeInBytes = maxSizeInBytes;
        this.expectedPartitions = Integer.MAX_VALUE;
        this.stats = requireNonNull(stats, "stats is null");
        this.runtimeStats = requireNonNull(runtimeStats, "runtimeStats is null");
        this.extendedMetrics = extendedMetrics;

        this.constraintByFilterIdFuture = new CompletableFuture<>();
    }

    public RuntimeStats getRuntimeStats()
    {
        return runtimeStats;
    }

    public synchronized void setExpectedPartitions(int expectedPartitions)
    {
        verify(expectedPartitions > 0, "expectedPartitions must be positive");
        this.expectedPartitions = expectedPartitions;
        runtimeStats.addMetricValue(DYNAMIC_FILTER_EXPECTED_PARTITIONS, NONE, expectedPartitions);
        if (!filterId.isEmpty()) {
            runtimeStats.addMetricValue(format("%s[%s]", DYNAMIC_FILTER_EXPECTED_PARTITIONS, filterId), NONE, expectedPartitions);
        }
        tryCompleteResolution();
    }

    /**
     * Must be called when wired to a split source, not at registration time,
     * since the filter may be pre-registered well before the split source exists.
     */
    public void startTimeout()
    {
        if (timeoutStarted.compareAndSet(false, true)) {
            long timeoutMs = waitTimeout.toMillis();
            if (timeoutMs > 0) {
                constraintByFilterIdFuture.completeOnTimeout(TupleDomain.all(), timeoutMs, TimeUnit.MILLISECONDS);
                if (extendedMetrics) {
                    constraintByFilterIdFuture.whenComplete((result, throwable) -> {
                        if (!fullyResolved) {
                            onTimeout();
                        }
                    });
                }
            }
        }
    }

    public Duration getWaitTimeout()
    {
        return waitTimeout;
    }

    public String getFilterId()
    {
        return filterId;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public synchronized void setProbeColumnDomain(Domain domain)
    {
        this.probeColumnDomain = requireNonNull(domain, "domain is null");
    }

    public boolean isComplete()
    {
        return fullyResolved;
    }

    /**
     * Records a contribution from {@code sourceTaskId}. Replaces any prior
     * contribution from the same task (progressive narrowing emits multiple
     * intermediate domains during the build). When {@code isFinal} is true
     * the task is added to {@link #finalizedTasks} and the merge may complete.
     */
    public synchronized void addPartitionByFilterId(TaskId sourceTaskId, TupleDomain<String> tupleDomain, boolean isFinal)
    {
        requireNonNull(sourceTaskId, "sourceTaskId is null");
        requireNonNull(tupleDomain, "tupleDomain is null");
        if (!collectionStarted) {
            collectionStartNanos = System.nanoTime();
            collectionStarted = true;
        }

        // Replace, don't append: progressive-narrowing partials from the same
        // task supersede each other. The final emission is the source of truth.
        latestByTask.put(sourceTaskId, tupleDomain);
        if (isFinal) {
            finalizedTasks.add(sourceTaskId);
        }

        runtimeStats.addMetricValue(DYNAMIC_FILTER_PARTITIONS_RECEIVED, NONE, 1);
        if (!filterId.isEmpty()) {
            runtimeStats.addMetricValue(format("%s[%s]", DYNAMIC_FILTER_PARTITIONS_RECEIVED, filterId), NONE, 1);
        }

        tryCompleteResolution();
    }

    /**
     * Marks {@code sourceTaskId} as finalized without changing its latest
     * contribution. Used when a fetcher response declares a filter id complete
     * via {@code completedFilterIds} after the domain was already delivered as
     * a partial contribution. If the task has never sent a contribution this
     * is a no-op; callers must instead deliver {@code TupleDomain.none()} with
     * {@code isFinal=true} via {@link #addPartitionByFilterId} for the empty-build
     * case.
     */
    public synchronized void markFinalForTask(TaskId sourceTaskId)
    {
        requireNonNull(sourceTaskId, "sourceTaskId is null");
        if (latestByTask.containsKey(sourceTaskId) && finalizedTasks.add(sourceTaskId)) {
            tryCompleteResolution();
        }
    }

    @GuardedBy("this")
    private void tryCompleteResolution()
    {
        if (constraintByFilterIdFuture.isDone() || finalizedTasks.size() < expectedPartitions) {
            return;
        }

        TupleDomain<String> union = TupleDomain.columnWiseUnion(ImmutableList.copyOf(latestByTask.values()));
        mergedConstraint = collapseIfOversized(union);
        maybeShortCircuit();
        fullyResolved = true;
        constraintByFilterIdFuture.complete(mergedConstraint);
        recordCollectionCompleted();
    }

    private TupleDomain<String> collapseIfOversized(TupleDomain<String> tupleDomain)
    {
        if (estimateRetainedSizeInBytes(tupleDomain) > maxSizeInBytes) {
            runtimeStats.addMetricValue(DYNAMIC_FILTER_COORDINATOR_FALLBACK_TO_RANGE, NONE, 1);
            if (!filterId.isEmpty()) {
                runtimeStats.addMetricValue(format("%s[%s]", DYNAMIC_FILTER_COORDINATOR_FALLBACK_TO_RANGE, filterId), NONE, 1);
            }
            return collapseToRange(tupleDomain);
        }
        return tupleDomain;
    }

    static long estimateRetainedSizeInBytes(TupleDomain<String> tupleDomain)
    {
        if (tupleDomain.isNone() || tupleDomain.isAll() || !tupleDomain.getDomains().isPresent()) {
            return 0;
        }
        long totalSize = 0;
        for (Domain domain : tupleDomain.getDomains().get().values()) {
            for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
                totalSize += range.getLow().getValueBlock()
                        .map(Block::getRetainedSizeInBytes)
                        .orElse(0L);
                totalSize += range.getHigh().getValueBlock()
                        .map(Block::getRetainedSizeInBytes)
                        .orElse(0L);
            }
        }
        return totalSize;
    }

    static TupleDomain<String> collapseToRange(TupleDomain<String> tupleDomain)
    {
        if (tupleDomain.isNone() || tupleDomain.isAll() || !tupleDomain.getDomains().isPresent()) {
            return tupleDomain;
        }
        ImmutableMap.Builder<String, Domain> collapsed = ImmutableMap.builder();
        for (Map.Entry<String, Domain> entry : tupleDomain.getDomains().get().entrySet()) {
            Domain domain = entry.getValue();
            ValueSet values = domain.getValues();
            if (values instanceof SortedRangeSet) {
                SortedRangeSet sortedRangeSet = (SortedRangeSet) values;
                if (sortedRangeSet.getRangeCount() > 1) {
                    collapsed.put(entry.getKey(), Domain.create(ValueSet.ofRanges(sortedRangeSet.getSpan()), domain.isNullAllowed()));
                    continue;
                }
            }
            collapsed.put(entry.getKey(), domain);
        }
        return TupleDomain.withColumnDomains(collapsed.build());
    }

    private void maybeShortCircuit()
    {
        if (probeColumnDomain == null || mergedConstraint.isAll() || mergedConstraint.isNone()) {
            return;
        }
        if (!mergedConstraint.getDomains().isPresent()) {
            return;
        }
        Map<String, Domain> domains = mergedConstraint.getDomains().get();
        if (domains.size() != 1) {
            return;
        }
        Domain filterDomain = domains.values().iterator().next();
        if (filterDomain.contains(probeColumnDomain)) {
            mergedConstraint = TupleDomain.all();
            runtimeStats.addMetricValue(DYNAMIC_FILTER_SHORT_CIRCUITED, NONE, 1);
            if (!filterId.isEmpty()) {
                runtimeStats.addMetricValue(format("%s[%s]", DYNAMIC_FILTER_SHORT_CIRCUITED, filterId), NONE, 1);
            }
        }
    }

    private void recordCollectionCompleted()
    {
        stats.getFilterCollectionCompleted().update(1);
        recordCollectionTime();
        if (extendedMetrics && !filterId.isEmpty()) {
            runtimeStats.addMetricValue(format("%s[%s]", DYNAMIC_FILTER_DOMAIN_RANGE_COUNT, filterId), NONE, computeRangeCount(mergedConstraint));
        }
    }

    private synchronized void onTimeout()
    {
        if (!filterId.isEmpty()) {
            runtimeStats.addMetricValue(format("%s[%s]", DYNAMIC_FILTER_TIMED_OUT, filterId), NONE, 1);
        }
        stats.getFilterCollectionTimedOut().update(1);
        recordCollectionTime();
    }

    private void recordCollectionTime()
    {
        if (collectionStarted && !collectionTimeRecorded) {
            collectionTimeRecorded = true;
            long elapsedNanos = System.nanoTime() - collectionStartNanos;
            runtimeStats.addMetricValue(DYNAMIC_FILTER_COLLECTION_TIME_NANOS, NANO, elapsedNanos);
            if (!filterId.isEmpty()) {
                runtimeStats.addMetricValue(format("%s[%s]", DYNAMIC_FILTER_COLLECTION_TIME_NANOS, filterId), NANO, elapsedNanos);
            }
        }
    }

    static long computeRangeCount(TupleDomain<String> tupleDomain)
    {
        if (tupleDomain.isNone() || !tupleDomain.getDomains().isPresent()) {
            return 0;
        }
        return tupleDomain.getDomains().get().values().stream()
                .mapToLong(domain -> domain.getValues().getRanges().getRangeCount())
                .sum();
    }

    public CompletableFuture<?> isBlocked()
    {
        if (constraintByFilterIdFuture.isDone()) {
            return NOT_BLOCKED;
        }
        return constraintByFilterIdFuture.thenApply(v -> null);
    }

    /**
     * Returns all() until ALL expected partitions arrive to avoid pruning splits
     * for not-yet-reported workers.
     */
    public synchronized TupleDomain<String> getCurrentConstraintByColumnName()
    {
        if (!fullyResolved || mergedConstraint == null) {
            return TupleDomain.all();
        }
        return translateToColumnName(mergedConstraint);
    }

    private TupleDomain<String> translateToColumnName(TupleDomain<String> filterIdDomain)
    {
        if (columnName.isEmpty() || filterIdDomain.isAll() || filterIdDomain.isNone() ||
                !filterIdDomain.getDomains().isPresent()) {
            return filterIdDomain;
        }

        Map<String, Domain> domains = filterIdDomain.getDomains().get();
        verify(domains.size() == 1, "Expected single-column filter but got %s entries", domains.size());
        Domain domain = domains.values().iterator().next();
        if (domain.isAll()) {
            return TupleDomain.all();
        }
        return TupleDomain.withColumnDomains(ImmutableMap.of(columnName, domain));
    }

    /**
     * Registers a callback that fires when this filter is fully resolved
     * (all expected partitions received, NOT on timeout).
     * The callback receives (filterId, constraintByColumnName).
     */
    public void onFullyResolved(BiConsumer<String, TupleDomain<String>> callback)
    {
        constraintByFilterIdFuture.whenComplete((domain, throwable) -> {
            if (fullyResolved && throwable == null) {
                callback.accept(filterId, getCurrentConstraintByColumnName());
            }
        });
    }

    public synchronized boolean hasData()
    {
        return !latestByTask.isEmpty();
    }

    public static DynamicFilter createDisabled()
    {
        return DynamicFilter.EMPTY;
    }

    @Override
    public synchronized String toString()
    {
        return toStringHelper(this)
                .add("filterId", filterId)
                .add("columnName", columnName)
                .add("waitTimeout", waitTimeout)
                .add("expectedPartitions", expectedPartitions)
                .add("contributingTasks", latestByTask.size())
                .add("finalizedTasks", finalizedTasks.size())
                .add("complete", fullyResolved)
                .toString();
    }
}
