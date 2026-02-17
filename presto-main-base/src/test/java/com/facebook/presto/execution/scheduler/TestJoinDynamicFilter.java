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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_COLLECTION_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_DOMAIN_RANGE_COUNT;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PARTITIONS_RECEIVED;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_TIMED_OUT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestJoinDynamicFilter
{
    private static final String DYNAMIC_FILTER_COLLECTION_TIME_NANOS_TEMPLATE = DYNAMIC_FILTER_COLLECTION_TIME_NANOS + "[%s]";
    public static final String DYNAMIC_FILTER_PARTITIONS_RECEIVED_TEMPLATE = DYNAMIC_FILTER_PARTITIONS_RECEIVED + "[%s]";
    private static final String DYNAMIC_FILTER_TIMED_OUT_TEMPLATE = DYNAMIC_FILTER_TIMED_OUT + "[%s]";
    private static final String DYNAMIC_FILTER_DOMAIN_RANGE_COUNT_TEMPLATE = DYNAMIC_FILTER_DOMAIN_RANGE_COUNT + "[%s]";
    private static final Duration DEFAULT_TIMEOUT = new Duration(2, TimeUnit.SECONDS);

    @Test
    public void testPerFilterMetrics()
    {
        RuntimeStats runtimeStats = new RuntimeStats();

        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "column_a",
                DEFAULT_TIMEOUT,
                new DynamicFilterStats(),
                runtimeStats,
                true);
        filter.setExpectedPartitions(2);

        // Add two partitions keyed by filter ID to trigger per-filter metrics
        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 10L))));
        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 20L))));

        assertTrue(filter.isComplete());

        // Verify aggregate metrics
        assertTrue(runtimeStats.getMetrics().containsKey(DYNAMIC_FILTER_PARTITIONS_RECEIVED),
                "Aggregate PARTITIONS_RECEIVED should be present");
        assertEquals(runtimeStats.getMetrics().get(DYNAMIC_FILTER_PARTITIONS_RECEIVED).getSum(), 2);

        // Verify per-filter metrics
        String perFilterPartitions = format(DYNAMIC_FILTER_PARTITIONS_RECEIVED_TEMPLATE, "549");
        assertTrue(runtimeStats.getMetrics().containsKey(perFilterPartitions),
                "Per-filter PARTITIONS_RECEIVED[549] should be present");
        assertEquals(runtimeStats.getMetrics().get(perFilterPartitions).getSum(), 2);

        // Verify per-filter collection time
        String perFilterCollectionTime = format(DYNAMIC_FILTER_COLLECTION_TIME_NANOS_TEMPLATE, "549");
        assertTrue(runtimeStats.getMetrics().containsKey(perFilterCollectionTime),
                "Per-filter COLLECTION_TIME_NANOS[549] should be present");
        assertTrue(runtimeStats.getMetrics().get(perFilterCollectionTime).getSum() > 0,
                "Per-filter collection time should be positive");

        String perFilterRangeCount = format(DYNAMIC_FILTER_DOMAIN_RANGE_COUNT_TEMPLATE, "549");
        assertTrue(runtimeStats.getMetrics().containsKey(perFilterRangeCount),
                "Per-filter DOMAIN_RANGE_COUNT[549] should be present with extendedMetrics");
        assertEquals(runtimeStats.getMetrics().get(perFilterRangeCount).getSum(), 2,
                "Domain range count should be 2 for two single-value partitions");
    }

    @Test
    public void testPeekFilterReturnsAllWhenNotResolved()
    {
        RuntimeStats runtimeStats = new RuntimeStats();

        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                DEFAULT_TIMEOUT,
                new DynamicFilterStats(),
                runtimeStats);
        filter.setExpectedPartitions(3);

        // No partitions received — should return all()
        assertEquals(filter.getCurrentConstraintByColumnName(), TupleDomain.all());
        assertFalse(filter.isComplete());

        // One partition received (partial) — should still return all()
        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 10L))));
        assertEquals(filter.getCurrentConstraintByColumnName(), TupleDomain.all());
        assertFalse(filter.isComplete());

        // Two partitions received (still partial) — should still return all()
        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 20L))));
        assertEquals(filter.getCurrentConstraintByColumnName(), TupleDomain.all());
        assertFalse(filter.isComplete());

        // All three partitions received — now returns actual constraint
        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 30L))));
        assertTrue(filter.isComplete());
        TupleDomain<String> constraint = filter.getCurrentConstraintByColumnName();
        assertFalse(constraint.isAll(), "Fully resolved filter should return actual constraint, not all()");
        assertEquals(
                constraint,
                TupleDomain.withColumnDomains(
                        ImmutableMap.of("col_a", Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L, 30L)))));
    }

    @Test
    public void testTimeoutDoesNotResolveFilter()
            throws Exception
    {
        RuntimeStats runtimeStats = new RuntimeStats();

        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                new Duration(100, TimeUnit.MILLISECONDS),
                new DynamicFilterStats(),
                runtimeStats);
        filter.setExpectedPartitions(2);

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 10L))));

        filter.startTimeout();
        Thread.sleep(300);

        // Future is done (timeout) but filter is NOT fully resolved
        assertFalse(filter.isComplete(), "Timeout should not mark filter as complete");
        assertEquals(filter.getCurrentConstraintByColumnName(), TupleDomain.all(),
                "Partial data should not be exposed after timeout");
    }

    @Test
    public void testNoPerFilterMetricsWithEmptyFilterId()
    {
        RuntimeStats runtimeStats = new RuntimeStats();

        // Empty filterId — should not emit per-filter metrics
        JoinDynamicFilter filter = new JoinDynamicFilter(
                "",
                "",
                DEFAULT_TIMEOUT,
                new DynamicFilterStats(),
                runtimeStats);
        filter.setExpectedPartitions(1);

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("", Domain.singleValue(INTEGER, 10L))));

        assertTrue(filter.isComplete());

        // Aggregate metrics should be present
        assertTrue(runtimeStats.getMetrics().containsKey(DYNAMIC_FILTER_PARTITIONS_RECEIVED));

        // No per-filter metrics with empty filterId
        assertFalse(runtimeStats.getMetrics().keySet().stream().anyMatch(k -> k.contains("[")),
                "No bracket-notation metrics should be present with empty filterId");
    }

    @Test
    public void testGetFilterId()
    {
        JoinDynamicFilter defaultFilter = new JoinDynamicFilter(DEFAULT_TIMEOUT);
        assertEquals(defaultFilter.getFilterId(), "");

        RuntimeStats runtimeStats = new RuntimeStats();
        JoinDynamicFilter namedFilter = new JoinDynamicFilter(
                "549",
                "column_a",
                DEFAULT_TIMEOUT,
                new DynamicFilterStats(),
                runtimeStats);
        assertEquals(namedFilter.getFilterId(), "549");
    }

    @Test
    public void testIsBlockedBeforeAndAfterResolution()
    {
        RuntimeStats runtimeStats = new RuntimeStats();

        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                DEFAULT_TIMEOUT,
                new DynamicFilterStats(),
                runtimeStats);
        filter.setExpectedPartitions(1);

        CompletableFuture<?> blocked = filter.isBlocked();
        assertFalse(blocked.isDone());

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 10L))));
        assertTrue(blocked.isDone());

        assertEquals(filter.isBlocked(), DynamicFilter.NOT_BLOCKED);
    }

    @Test
    public void testGetWaitTimeout()
    {
        Duration timeout = new Duration(5, TimeUnit.SECONDS);
        JoinDynamicFilter filter = new JoinDynamicFilter(timeout);

        assertEquals(filter.getWaitTimeout(), timeout);
    }

    @Test
    public void testCreateDisabled()
    {
        assertEquals(JoinDynamicFilter.createDisabled(), DynamicFilter.EMPTY);
    }

    @Test
    public void testTimeoutEmitsMetric()
            throws Exception
    {
        RuntimeStats runtimeStats = new RuntimeStats();
        DynamicFilterStats stats = new DynamicFilterStats();

        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                new Duration(100, TimeUnit.MILLISECONDS),
                stats,
                runtimeStats,
                true);
        filter.setExpectedPartitions(2);

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 10L))));

        filter.startTimeout();
        Thread.sleep(300);

        assertFalse(filter.isComplete(), "Timeout should not mark filter as complete");

        String timeoutKey = format(DYNAMIC_FILTER_TIMED_OUT_TEMPLATE, "549");
        assertTrue(runtimeStats.getMetrics().containsKey(timeoutKey),
                "Timeout metric should be emitted");
        assertEquals(runtimeStats.getMetrics().get(timeoutKey).getSum(), 1);

        assertEquals(stats.getFilterCollectionTimedOut().getTotalCount(), 1);

        String collectionTimeKey = format(DYNAMIC_FILTER_COLLECTION_TIME_NANOS_TEMPLATE, "549");
        assertTrue(runtimeStats.getMetrics().containsKey(collectionTimeKey),
                "Collection time should be emitted on timeout with extendedMetrics");
        assertTrue(runtimeStats.getMetrics().get(collectionTimeKey).getSum() > 0,
                "Collection time should be positive");
    }

    @Test
    public void testNoTimeoutMetricOnSuccess()
    {
        RuntimeStats runtimeStats = new RuntimeStats();

        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                DEFAULT_TIMEOUT,
                new DynamicFilterStats(),
                runtimeStats,
                true);
        filter.setExpectedPartitions(1);

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 10L))));

        assertTrue(filter.isComplete());

        String timeoutKey = format(DYNAMIC_FILTER_TIMED_OUT_TEMPLATE, "549");
        assertFalse(runtimeStats.getMetrics().containsKey(timeoutKey),
                "Timeout metric should not be emitted on successful completion");
    }

    @Test
    public void testDomainRangeCountForNone()
    {
        RuntimeStats runtimeStats = new RuntimeStats();

        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                DEFAULT_TIMEOUT,
                new DynamicFilterStats(),
                runtimeStats,
                true);
        filter.setExpectedPartitions(1);

        filter.addPartitionByFilterId(TupleDomain.none());

        assertTrue(filter.isComplete());

        String rangeCountKey = format(DYNAMIC_FILTER_DOMAIN_RANGE_COUNT_TEMPLATE, "549");
        assertTrue(runtimeStats.getMetrics().containsKey(rangeCountKey),
                "Domain range count should be emitted for none()");
        assertEquals(runtimeStats.getMetrics().get(rangeCountKey).getSum(), 0,
                "Domain range count should be 0 for none() domain");
    }

    @Test
    public void testComputeRangeCount()
    {
        assertEquals(JoinDynamicFilter.computeRangeCount(TupleDomain.none()), 0);
        assertEquals(JoinDynamicFilter.computeRangeCount(TupleDomain.all()), 0);

        TupleDomain<String> singleValue = TupleDomain.withColumnDomains(
                ImmutableMap.of("col", Domain.singleValue(INTEGER, 10L)));
        assertEquals(JoinDynamicFilter.computeRangeCount(singleValue), 1);

        TupleDomain<String> multiValue = TupleDomain.withColumnDomains(
                ImmutableMap.of("col", Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L, 30L))));
        assertEquals(JoinDynamicFilter.computeRangeCount(multiValue), 3);
    }
}
