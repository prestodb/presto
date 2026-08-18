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
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.connector.DynamicFilter;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.ThreadSafe;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
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
 * Collects per-partition {@link RuntimeFilter} contributions from build-side tasks for a
 * single join dynamic filter, merges them on completion, and exposes the result as a
 * {@link TupleDomain} keyed by probe column name.
 *
 * <p>Each {@code JoinDynamicFilter} targets exactly one join column ({@code columnName})
 * and one filter ID ({@code filterId}). It accumulates one {@link RuntimeFilter} per
 * build partition via {@link #addPartitionByFilterId}. When all {@code expectedPartitions}
 * have arrived, the contributions are union-merged and, if the result exceeds
 * {@code maxSizeInBytes}, collapsed to a min/max range.
 *
 * <p>An adaptive timeout fires after {@code waitTimeout}; if new contributions arrived
 * since the last tick, up to {@code maxWaitExtensions} additional cycles are granted.
 * On expiry the future completes with {@code TupleDomain.all()} so the probe side
 * can proceed without a filter.
 *
 * <p>This class does not implement {@link com.facebook.presto.spi.connector.DynamicFilter}
 * directly. The SPI-facing wrapper is {@link TableScanDynamicFilter}, which intersects
 * one or more {@code JoinDynamicFilter}s and exposes them to the connector split source.
 */
@ThreadSafe
public class JoinDynamicFilter
{
    // Injected by DynamicFilterService in PR4; shared daemon pool until then.
    static final ScheduledExecutorService DEFAULT_SCHEDULER =
            Executors.newSingleThreadScheduledExecutor(daemonThreadsNamed("join-dynamic-filter-timeout-%s"));

    private final ScheduledExecutorService timeoutScheduler;
    private final String filterId;
    private final String columnName;
    private final Duration waitTimeout;
    private final int maxWaitExtensions;
    private final long maxSizeInBytes;
    private final DynamicFilterStats stats;
    private final RuntimeStats runtimeStats;
    private final boolean extendedMetrics;

    @GuardedBy("this")
    private final List<RuntimeFilter> partitionsByFilterId = new ArrayList<>();
    private final CompletableFuture<RuntimeFilter> constraintByFilterIdFuture;

    private final AtomicBoolean timeoutStarted = new AtomicBoolean(false);

    @GuardedBy("this")
    private int expectedPartitions;

    private volatile boolean fullyResolved;

    @GuardedBy("this")
    private RuntimeFilter mergedConstraint;

    @GuardedBy("this")
    private Domain probeColumnDomain;

    @GuardedBy("this")
    private long collectionStartNanos;
    @GuardedBy("this")
    private boolean collectionStarted;
    @GuardedBy("this")
    private boolean collectionTimeRecorded;
    @GuardedBy("this")
    private int extensionsUsed;
    @GuardedBy("this")
    private int lastTickPartitionCount;

    public JoinDynamicFilter(
            String filterId,
            String columnName,
            Duration waitTimeout,
            long maxSizeInBytes,
            DynamicFilterStats stats,
            RuntimeStats runtimeStats,
            boolean extendedMetrics)
    {
        this(filterId, columnName, waitTimeout, 0, maxSizeInBytes, stats, runtimeStats, extendedMetrics, DEFAULT_SCHEDULER);
    }

    public JoinDynamicFilter(
            String filterId,
            String columnName,
            Duration waitTimeout,
            int maxWaitExtensions,
            long maxSizeInBytes,
            DynamicFilterStats stats,
            RuntimeStats runtimeStats,
            boolean extendedMetrics)
    {
        this(filterId, columnName, waitTimeout, maxWaitExtensions, maxSizeInBytes, stats, runtimeStats, extendedMetrics, DEFAULT_SCHEDULER);
    }

    // Package-private: for injection by DynamicFilterService (PR4) and tests.
    JoinDynamicFilter(
            String filterId,
            String columnName,
            Duration waitTimeout,
            int maxWaitExtensions,
            long maxSizeInBytes,
            DynamicFilterStats stats,
            RuntimeStats runtimeStats,
            boolean extendedMetrics,
            ScheduledExecutorService timeoutScheduler)
    {
        this.filterId = requireNonNull(filterId, "filterId is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.waitTimeout = requireNonNull(waitTimeout, "waitTimeout is null");
        verify(maxWaitExtensions >= 0, "maxWaitExtensions must be non-negative");
        this.maxWaitExtensions = maxWaitExtensions;
        this.maxSizeInBytes = maxSizeInBytes;
        this.expectedPartitions = Integer.MAX_VALUE;
        this.stats = requireNonNull(stats, "stats is null");
        this.runtimeStats = requireNonNull(runtimeStats, "runtimeStats is null");
        this.extendedMetrics = extendedMetrics;
        this.timeoutScheduler = requireNonNull(timeoutScheduler, "timeoutScheduler is null");

        this.constraintByFilterIdFuture = new CompletableFuture<>();
    }

    public RuntimeStats getRuntimeStats()
    {
        return runtimeStats;
    }

    public void setExpectedPartitions(int expectedPartitions)
    {
        RuntimeFilter resolved;
        synchronized (this) {
            verify(this.expectedPartitions == Integer.MAX_VALUE, "setExpectedPartitions already called");
            verify(expectedPartitions > 0, "expectedPartitions must be positive");
            this.expectedPartitions = expectedPartitions;
            runtimeStats.addMetricValue(DYNAMIC_FILTER_EXPECTED_PARTITIONS, NONE, expectedPartitions);
            if (!filterId.isEmpty()) {
                runtimeStats.addMetricValue(format("%s[%s]", DYNAMIC_FILTER_EXPECTED_PARTITIONS, filterId), NONE, expectedPartitions);
            }
            resolved = tryCompleteResolution();
        }
        if (resolved != null) {
            constraintByFilterIdFuture.complete(resolved);
        }
    }

    // Must be called when wired to a split source, not at construction time.
    public void startTimeout()
    {
        if (timeoutStarted.compareAndSet(false, true)) {
            long timeoutMs = waitTimeout.toMillis();
            if (timeoutMs > 0) {
                // Baseline so pre-startTimeout contributions aren't credited as new progress on the first tick.
                synchronized (this) {
                    lastTickPartitionCount = partitionsByFilterId.size();
                }
                scheduleTick(timeoutMs);
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

    public void addPartitionByFilterId(TupleDomain<String> tupleDomain)
    {
        requireNonNull(tupleDomain, "tupleDomain is null");
        tupleDomain.getDomains().ifPresent(domains ->
                verify(domains.size() == 1, "Expected single-column filter domain but got %s columns", domains.size()));
        addPartitionByFilterId(new DomainRuntimeFilter(tupleDomain));
    }

    public void addPartitionByFilterId(RuntimeFilter filter)
    {
        requireNonNull(filter, "filter is null");
        RuntimeFilter resolved;
        synchronized (this) {
            if (!collectionStarted) {
                collectionStartNanos = System.nanoTime();
                collectionStarted = true;
            }

            partitionsByFilterId.add(filter);

            runtimeStats.addMetricValue(DYNAMIC_FILTER_PARTITIONS_RECEIVED, NONE, 1);
            if (!filterId.isEmpty()) {
                runtimeStats.addMetricValue(format("%s[%s]", DYNAMIC_FILTER_PARTITIONS_RECEIVED, filterId), NONE, 1);
            }

            resolved = tryCompleteResolution();
        }
        if (resolved != null) {
            constraintByFilterIdFuture.complete(resolved);
        }
    }

    // Returns the resolved filter to complete outside the lock, or null if not yet ready.
    @GuardedBy("this")
    private RuntimeFilter tryCompleteResolution()
    {
        if (constraintByFilterIdFuture.isDone() || partitionsByFilterId.size() < expectedPartitions) {
            return null;
        }

        RuntimeFilter union = partitionsByFilterId.get(0);
        for (int i = 1; i < partitionsByFilterId.size(); i++) {
            union = union.mergeWith(partitionsByFilterId.get(i));
        }
        mergedConstraint = collapseIfOversized(union);
        maybeShortCircuit();
        fullyResolved = true;
        recordCollectionCompleted();
        return mergedConstraint;
    }

    private RuntimeFilter collapseIfOversized(RuntimeFilter filter)
    {
        if (filter.estimatedRetainedSizeInBytes() <= maxSizeInBytes) {
            return filter;
        }
        verify(filter instanceof DomainRuntimeFilter,
                "Cannot collapse oversized %s; add collapse support before introducing new RuntimeFilter types",
                filter.getClass().getSimpleName());
        runtimeStats.addMetricValue(DYNAMIC_FILTER_COORDINATOR_FALLBACK_TO_RANGE, NONE, 1);
        if (!filterId.isEmpty()) {
            runtimeStats.addMetricValue(format("%s[%s]", DYNAMIC_FILTER_COORDINATOR_FALLBACK_TO_RANGE, filterId), NONE, 1);
        }
        return new DomainRuntimeFilter(collapseToRange(((DomainRuntimeFilter) filter).getDomain()));
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
        if (probeColumnDomain == null || !(mergedConstraint instanceof DomainRuntimeFilter)) {
            return;
        }
        TupleDomain<String> tupleDomain = ((DomainRuntimeFilter) mergedConstraint).getDomain();
        if (tupleDomain.isAll() || tupleDomain.isNone() || !tupleDomain.getDomains().isPresent()) {
            return;
        }
        Map<String, Domain> domains = tupleDomain.getDomains().get();
        verify(domains.size() == 1,
                "Expected single-column domain in maybeShortCircuit but got %s columns: %s",
                domains.size(), domains.keySet());
        Domain filterDomain = domains.values().iterator().next();
        if (filterDomain.contains(probeColumnDomain)) {
            mergedConstraint = new DomainRuntimeFilter(TupleDomain.all());
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
            long rangeCount = (mergedConstraint instanceof DomainRuntimeFilter)
                    ? computeRangeCount(((DomainRuntimeFilter) mergedConstraint).getDomain())
                    : 0;
            runtimeStats.addMetricValue(format("%s[%s]", DYNAMIC_FILTER_DOMAIN_RANGE_COUNT, filterId), NONE, rangeCount);
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

    @GuardedBy("this")
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
                .filter(domain -> domain.getValues() instanceof SortedRangeSet)
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

    /** Returns all() until fully resolved. */
    public synchronized TupleDomain<String> getCurrentConstraintByColumnName()
    {
        if (!fullyResolved || mergedConstraint == null) {
            return TupleDomain.all();
        }
        Type type = probeColumnDomain != null ? probeColumnDomain.getType() : null;
        return mergedConstraint.toTupleDomain(columnName, type);
    }

    /** Fires only on normal completion, not on timeout. */
    public void onFullyResolved(BiConsumer<String, RuntimeFilter> callback)
    {
        constraintByFilterIdFuture.whenComplete((filter, throwable) -> {
            if (fullyResolved && throwable == null) {
                callback.accept(filterId, filter);
            }
        });
    }

    public synchronized boolean hasData()
    {
        return !partitionsByFilterId.isEmpty();
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
                .add("receivedPartitions", partitionsByFilterId.size())
                .add("complete", fullyResolved)
                .toString();
    }

    private void scheduleTick(long timeoutMs)
    {
        timeoutScheduler.schedule(this::onTick, timeoutMs, TimeUnit.MILLISECONDS);
    }

    private void onTick()
    {
        boolean reschedule;
        synchronized (this) {
            if (constraintByFilterIdFuture.isDone()) {
                return;
            }
            int currentReceived = partitionsByFilterId.size();
            boolean progress = currentReceived > lastTickPartitionCount;
            lastTickPartitionCount = currentReceived;
            if (progress && extensionsUsed < maxWaitExtensions) {
                extensionsUsed++;
                reschedule = true;
            }
            else {
                // Finalize inside the monitor so a concurrent addPartitionByFilterId either
                // resolves the filter first (and this tick observes isDone) or sees the
                // future already completed and bails — partial state is never exposed.
                constraintByFilterIdFuture.complete(new DomainRuntimeFilter(TupleDomain.all()));
                onTimeout();
                reschedule = false;
            }
        }
        if (reschedule) {
            scheduleTick(waitTimeout.toMillis());
        }
    }
}
