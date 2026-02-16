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
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.connector.DynamicFilter;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_COLLECTION_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_EXPECTED_PARTITIONS;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PARTITIONS_RECEIVED;
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
 */
@ThreadSafe
public class JoinDynamicFilter
{
    private final String filterId;
    private final String columnName;
    private final Duration waitTimeout;
    private final DynamicFilterStats stats;
    private final Optional<RuntimeStats> runtimeStats;

    @GuardedBy("this")
    private final List<TupleDomain<String>> partitionsByFilterId;
    private final CompletableFuture<TupleDomain<String>> constraintByFilterIdFuture;

    private final AtomicBoolean timeoutStarted = new AtomicBoolean(false);

    @GuardedBy("this")
    private int expectedPartitions;

    private volatile boolean fullyResolved;

    @GuardedBy("this")
    private TupleDomain<String> mergedConstraint;

    @GuardedBy("this")
    private long collectionStartNanos;
    @GuardedBy("this")
    private boolean collectionStarted;

    public JoinDynamicFilter(Duration waitTimeout)
    {
        this("", "", waitTimeout, new DynamicFilterStats(), Optional.empty());
    }

    public JoinDynamicFilter(
            String filterId,
            String columnName,
            Duration waitTimeout,
            DynamicFilterStats stats,
            Optional<RuntimeStats> runtimeStats)
    {
        this.filterId = requireNonNull(filterId, "filterId is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.waitTimeout = requireNonNull(waitTimeout, "waitTimeout is null");
        this.expectedPartitions = Integer.MAX_VALUE;
        this.stats = requireNonNull(stats, "stats is null");
        this.runtimeStats = requireNonNull(runtimeStats, "runtimeStats is null");

        this.partitionsByFilterId = new ArrayList<>();
        this.constraintByFilterIdFuture = new CompletableFuture<>();
    }

    public Optional<RuntimeStats> getRuntimeStats()
    {
        return runtimeStats;
    }

    public synchronized void setExpectedPartitions(int expectedPartitions)
    {
        verify(expectedPartitions > 0, "expectedPartitions must be positive");
        this.expectedPartitions = expectedPartitions;
        runtimeStats.ifPresent(rs -> {
            rs.addMetricValue(DYNAMIC_FILTER_EXPECTED_PARTITIONS, NONE, expectedPartitions);
            if (!filterId.isEmpty()) {
                rs.addMetricValue(format("%s[%s]", DYNAMIC_FILTER_EXPECTED_PARTITIONS, filterId), NONE, expectedPartitions);
            }
        });
        if (!constraintByFilterIdFuture.isDone() && partitionsByFilterId.size() >= expectedPartitions) {
            mergedConstraint = TupleDomain.columnWiseUnion(partitionsByFilterId);
            fullyResolved = true;
            constraintByFilterIdFuture.complete(mergedConstraint);
            recordCollectionCompleted();
        }
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

    public boolean isComplete()
    {
        return fullyResolved;
    }

    public synchronized void addPartitionByFilterId(TupleDomain<String> tupleDomain)
    {
        requireNonNull(tupleDomain, "tupleDomain is null");
        if (!collectionStarted) {
            collectionStartNanos = System.nanoTime();
            collectionStarted = true;
        }

        // Accept partitions even after future completion for getCurrentConstraintByColumnName()
        partitionsByFilterId.add(tupleDomain);
        runtimeStats.ifPresent(rs -> {
            rs.addMetricValue(DYNAMIC_FILTER_PARTITIONS_RECEIVED, NONE, 1);
            if (!filterId.isEmpty()) {
                rs.addMetricValue(format("%s[%s]", DYNAMIC_FILTER_PARTITIONS_RECEIVED, filterId), NONE, 1);
            }
        });

        if (!constraintByFilterIdFuture.isDone() && partitionsByFilterId.size() >= expectedPartitions) {
            mergedConstraint = TupleDomain.columnWiseUnion(partitionsByFilterId);
            fullyResolved = true;
            constraintByFilterIdFuture.complete(mergedConstraint);
            recordCollectionCompleted();
        }
    }

    private void recordCollectionCompleted()
    {
        stats.getFilterCollectionCompleted().update(1);
        if (collectionStarted) {
            long elapsedNanos = System.nanoTime() - collectionStartNanos;
            runtimeStats.ifPresent(rs -> {
                rs.addMetricValue(DYNAMIC_FILTER_COLLECTION_TIME_NANOS, NANO, elapsedNanos);
                if (!filterId.isEmpty()) {
                    rs.addMetricValue(format("%s[%s]", DYNAMIC_FILTER_COLLECTION_TIME_NANOS, filterId), NANO, elapsedNanos);
                }
            });
        }
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
        return TupleDomain.withColumnDomains(ImmutableMap.of(columnName, domain));
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
    public String toString()
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
}
