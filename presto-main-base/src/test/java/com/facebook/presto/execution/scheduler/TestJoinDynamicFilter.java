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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_COLLECTION_TIME_NANOS;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_COORDINATOR_FALLBACK_TO_RANGE;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_DOMAIN_RANGE_COUNT;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PARTITIONS_RECEIVED;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_SHORT_CIRCUITED;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_TIMED_OUT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestJoinDynamicFilter
{
    private static final String DYNAMIC_FILTER_COLLECTION_TIME_NANOS_TEMPLATE = DYNAMIC_FILTER_COLLECTION_TIME_NANOS + "[%s]";
    private static final String DYNAMIC_FILTER_PARTITIONS_RECEIVED_TEMPLATE = DYNAMIC_FILTER_PARTITIONS_RECEIVED + "[%s]";
    private static final String DYNAMIC_FILTER_TIMED_OUT_TEMPLATE = DYNAMIC_FILTER_TIMED_OUT + "[%s]";
    private static final String DYNAMIC_FILTER_DOMAIN_RANGE_COUNT_TEMPLATE = DYNAMIC_FILTER_DOMAIN_RANGE_COUNT + "[%s]";
    private static final Duration DEFAULT_TIMEOUT = new Duration(2, TimeUnit.SECONDS);
    private static final long DEFAULT_MAX_SIZE_BYTES = 1_048_576L; // 1 MB

    @Test
    public void testPerFilterMetrics()
    {
        RuntimeStats runtimeStats = new RuntimeStats();

        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "column_a",
                DEFAULT_TIMEOUT,
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterServiceStats(),
                runtimeStats,
                true);
        filter.setExpectedPartitions(2);

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 10L))));
        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 20L))));

        assertTrue(filter.isComplete());

        assertTrue(runtimeStats.getMetrics().containsKey(DYNAMIC_FILTER_PARTITIONS_RECEIVED),
                "Aggregate PARTITIONS_RECEIVED should be present");
        assertEquals(runtimeStats.getMetrics().get(DYNAMIC_FILTER_PARTITIONS_RECEIVED).getSum(), 2);

        String perFilterPartitions = format(DYNAMIC_FILTER_PARTITIONS_RECEIVED_TEMPLATE, "549");
        assertTrue(runtimeStats.getMetrics().containsKey(perFilterPartitions),
                "Per-filter PARTITIONS_RECEIVED[549] should be present");
        assertEquals(runtimeStats.getMetrics().get(perFilterPartitions).getSum(), 2);

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
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterServiceStats(),
                runtimeStats,
                false);
        filter.setExpectedPartitions(3);

        // No partitions received — should return all()
        assertEquals(filter.getCurrentConstraintByColumnName(), TupleDomain.all());
        assertFalse(filter.isComplete());

        // One partition finalized (partial w.r.t. expected) — should still return all()
        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 10L))));
        assertEquals(filter.getCurrentConstraintByColumnName(), TupleDomain.all());
        assertFalse(filter.isComplete());

        // Two partitions finalized (still partial) — should still return all()
        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 20L))));
        assertEquals(filter.getCurrentConstraintByColumnName(), TupleDomain.all());
        assertFalse(filter.isComplete());

        // All three partitions finalized — now returns actual constraint
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
    {
        RuntimeStats runtimeStats = new RuntimeStats();
        ManualScheduler scheduler = new ManualScheduler();

        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                new Duration(100, TimeUnit.MILLISECONDS),
                0,
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterServiceStats(),
                runtimeStats,
                false,
                scheduler);
        filter.setExpectedPartitions(2);

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 10L))));

        filter.startTimeout();
        scheduler.tick();

        // Future is done (timeout) but filter is NOT fully resolved
        assertFalse(filter.isComplete(), "Timeout should not mark filter as complete");
        assertEquals(filter.getCurrentConstraintByColumnName(), TupleDomain.all(),
                "Partial data should not be exposed after timeout");
    }

    @Test
    public void testNoPerFilterMetricsWithEmptyFilterId()
    {
        RuntimeStats runtimeStats = new RuntimeStats();

        JoinDynamicFilter filter = new JoinDynamicFilter(
                "",
                "",
                DEFAULT_TIMEOUT,
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterServiceStats(),
                runtimeStats,
                false);
        filter.setExpectedPartitions(1);

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("", Domain.singleValue(INTEGER, 10L))));

        assertTrue(filter.isComplete());

        assertTrue(runtimeStats.getMetrics().containsKey(DYNAMIC_FILTER_PARTITIONS_RECEIVED));

        assertFalse(runtimeStats.getMetrics().keySet().stream().anyMatch(k -> k.contains("[")),
                "No bracket-notation metrics should be present with empty filterId");
    }

    @Test
    public void testGetFilterId()
    {
        JoinDynamicFilter defaultFilter = new JoinDynamicFilter(
                "",
                "",
                DEFAULT_TIMEOUT,
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterServiceStats(),
                new RuntimeStats(),
                false);
        assertEquals(defaultFilter.getFilterId(), "");

        RuntimeStats runtimeStats = new RuntimeStats();
        JoinDynamicFilter namedFilter = new JoinDynamicFilter(
                "549",
                "column_a",
                DEFAULT_TIMEOUT,
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterServiceStats(),
                runtimeStats,
                false);
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
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterServiceStats(),
                runtimeStats,
                false);
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
        JoinDynamicFilter filter = new JoinDynamicFilter(
                "",
                "",
                timeout,
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterServiceStats(),
                new RuntimeStats(),
                false);

        assertEquals(filter.getWaitTimeout(), timeout);
    }

    @Test
    public void testCreateDisabled()
    {
        assertEquals(JoinDynamicFilter.createDisabled(), DynamicFilter.EMPTY);
    }

    @Test
    public void testTimeoutEmitsMetric()
    {
        RuntimeStats runtimeStats = new RuntimeStats();
        DynamicFilterServiceStats stats = new DynamicFilterServiceStats();
        ManualScheduler scheduler = new ManualScheduler();

        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                new Duration(100, TimeUnit.MILLISECONDS),
                0,
                DEFAULT_MAX_SIZE_BYTES,
                stats,
                runtimeStats,
                true,
                scheduler);
        filter.setExpectedPartitions(2);

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 10L))));

        filter.startTimeout();
        scheduler.tick();

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
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterServiceStats(),
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
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterServiceStats(),
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

    @Test
    public void testNoCollapseWhenUnderSizeLimit()
    {
        RuntimeStats runtimeStats = new RuntimeStats();

        // Default max size (1 MB) — small discrete-value filters should complete normally,
        // not be collapsed to range.
        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                DEFAULT_TIMEOUT,
                1_048_576L,
                new DynamicFilterServiceStats(),
                runtimeStats,
                false);
        filter.setExpectedPartitions(1);

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L, 30L)))));

        assertTrue(filter.isComplete());

        TupleDomain<String> constraint = filter.getCurrentConstraintByColumnName();
        Domain domain = constraint.getDomains().get().get("col_a");
        assertEquals(domain.getValues().getRanges().getRangeCount(), 3);

        assertFalse(runtimeStats.getMetrics().containsKey(DYNAMIC_FILTER_COORDINATOR_FALLBACK_TO_RANGE),
                "Fallback metric should not be emitted when under size limit");
    }

    @Test
    public void testEstimateRetainedSizeInBytes()
    {
        assertEquals(new DomainRuntimeFilter(TupleDomain.none()).estimatedRetainedSizeInBytes(), 0);
        assertEquals(new DomainRuntimeFilter(TupleDomain.all()).estimatedRetainedSizeInBytes(), 0);

        // block storage for low + high markers
        TupleDomain<String> singleValue = TupleDomain.withColumnDomains(
                ImmutableMap.of("col", Domain.singleValue(INTEGER, 10L)));
        assertTrue(new DomainRuntimeFilter(singleValue).estimatedRetainedSizeInBytes() > 0);

        TupleDomain<String> multiValue = TupleDomain.withColumnDomains(
                ImmutableMap.of("col", Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L, 30L))));
        assertTrue(new DomainRuntimeFilter(multiValue).estimatedRetainedSizeInBytes() >
                new DomainRuntimeFilter(singleValue).estimatedRetainedSizeInBytes());
    }

    @Test
    public void testNoCollapseWithDefaultSize()
    {
        RuntimeStats runtimeStats = new RuntimeStats();

        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                DEFAULT_TIMEOUT,
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterServiceStats(),
                runtimeStats,
                false);
        filter.setExpectedPartitions(1);

        // 100 values still well under 1 MB — should not collapse
        List<Long> values = new ArrayList<>();
        for (long i = 0; i < 100; i++) {
            values.add(i);
        }
        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.multipleValues(INTEGER, values))));

        assertTrue(filter.isComplete());

        TupleDomain<String> constraint = filter.getCurrentConstraintByColumnName();
        Domain domain = constraint.getDomains().get().get("col_a");
        assertEquals(domain.getValues().getRanges().getRangeCount(), 100);
    }

    @Test
    public void testShortCircuitWhenBuildCoversProbe()
    {
        RuntimeStats runtimeStats = new RuntimeStats();

        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                DEFAULT_TIMEOUT,
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterServiceStats(),
                runtimeStats,
                false);

        filter.setProbeColumnDomain(Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)));
        filter.setExpectedPartitions(1);

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L, 4L, 5L)))));

        assertTrue(filter.isComplete());

        assertEquals(filter.getCurrentConstraintByColumnName(), TupleDomain.all(),
                "Filter should short-circuit to all() when build covers probe");

        assertTrue(runtimeStats.getMetrics().containsKey(DYNAMIC_FILTER_SHORT_CIRCUITED),
                "Aggregate short-circuit metric should be emitted");
        assertEquals(runtimeStats.getMetrics().get(DYNAMIC_FILTER_SHORT_CIRCUITED).getSum(), 1);
    }

    @Test
    public void testShortCircuitEmitsPerFilterMetric()
    {
        RuntimeStats runtimeStats = new RuntimeStats();

        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                DEFAULT_TIMEOUT,
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterServiceStats(),
                runtimeStats,
                false);

        filter.setProbeColumnDomain(Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)));
        filter.setExpectedPartitions(1);

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)))));

        assertTrue(filter.isComplete());
        assertEquals(filter.getCurrentConstraintByColumnName(), TupleDomain.all());

        String perFilterKey = format("%s[%s]", DYNAMIC_FILTER_SHORT_CIRCUITED, "549");
        assertTrue(runtimeStats.getMetrics().containsKey(perFilterKey),
                "Per-filter DYNAMIC_FILTER_SHORT_CIRCUITED[549] should be present");
        assertEquals(runtimeStats.getMetrics().get(perFilterKey).getSum(), 1);
    }

    @Test
    public void testNoShortCircuitWhenBuildDoesNotCoverProbe()
    {
        RuntimeStats runtimeStats = new RuntimeStats();

        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                DEFAULT_TIMEOUT,
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterServiceStats(),
                runtimeStats,
                false);

        filter.setProbeColumnDomain(Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L, 4L, 5L)));
        filter.setExpectedPartitions(1);

        // build covers [1,2,3] but probe has [1,2,3,4,5] — cannot short-circuit
        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)))));

        assertTrue(filter.isComplete());

        TupleDomain<String> constraint = filter.getCurrentConstraintByColumnName();
        assertFalse(constraint.isAll(),
                "Filter should NOT short-circuit when build does not cover probe");
        assertEquals(
                constraint,
                TupleDomain.withColumnDomains(
                        ImmutableMap.of("col_a", Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)))));

        assertFalse(runtimeStats.getMetrics().containsKey(DYNAMIC_FILTER_SHORT_CIRCUITED),
                "Short-circuit metric should NOT be emitted when build does not cover probe");
    }

    @Test
    public void testNoShortCircuitWithoutProbeColumnDomain()
    {
        RuntimeStats runtimeStats = new RuntimeStats();

        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                DEFAULT_TIMEOUT,
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterServiceStats(),
                runtimeStats,
                false);

        filter.setExpectedPartitions(1);

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)))));

        assertTrue(filter.isComplete());

        TupleDomain<String> constraint = filter.getCurrentConstraintByColumnName();
        assertFalse(constraint.isAll(),
                "Filter should NOT short-circuit without probeColumnDomain");
        assertEquals(
                constraint,
                TupleDomain.withColumnDomains(
                        ImmutableMap.of("col_a", Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)))));

        assertFalse(runtimeStats.getMetrics().containsKey(DYNAMIC_FILTER_SHORT_CIRCUITED));
    }

    @Test
    public void testShortCircuitViaSetExpectedPartitions()
    {
        RuntimeStats runtimeStats = new RuntimeStats();

        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                DEFAULT_TIMEOUT,
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterServiceStats(),
                runtimeStats,
                false);

        filter.setProbeColumnDomain(Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L)));

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)))));
        assertFalse(filter.isComplete());

        filter.setExpectedPartitions(1);
        assertTrue(filter.isComplete());

        assertEquals(filter.getCurrentConstraintByColumnName(), TupleDomain.all(),
                "Short-circuit should fire via setExpectedPartitions path");

        assertTrue(runtimeStats.getMetrics().containsKey(DYNAMIC_FILTER_SHORT_CIRCUITED));
    }

    @Test
    public void testShortCircuitWithExactMatch()
    {
        RuntimeStats runtimeStats = new RuntimeStats();

        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                DEFAULT_TIMEOUT,
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterServiceStats(),
                runtimeStats,
                false);

        Domain probeDomain = Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L, 30L));
        filter.setProbeColumnDomain(probeDomain);
        filter.setExpectedPartitions(1);

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L, 30L)))));

        assertTrue(filter.isComplete());
        assertEquals(filter.getCurrentConstraintByColumnName(), TupleDomain.all(),
                "Exact match of build and probe domains should short-circuit");
        assertTrue(runtimeStats.getMetrics().containsKey(DYNAMIC_FILTER_SHORT_CIRCUITED));
    }

    @Test
    public void testNoShortCircuitWhenBuildIsNone()
    {
        RuntimeStats runtimeStats = new RuntimeStats();

        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                DEFAULT_TIMEOUT,
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterServiceStats(),
                runtimeStats,
                false);

        filter.setProbeColumnDomain(Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)));
        filter.setExpectedPartitions(1);

        filter.addPartitionByFilterId(TupleDomain.none());

        assertTrue(filter.isComplete());

        // none() should NOT be short-circuited — it means the build was empty and
        // we should prune everything
        TupleDomain<String> constraint = filter.getCurrentConstraintByColumnName();
        assertTrue(constraint.isNone(),
                "none() constraint should be preserved (empty build prunes everything)");
        assertFalse(runtimeStats.getMetrics().containsKey(DYNAMIC_FILTER_SHORT_CIRCUITED));
    }

    @Test
    public void testSizeBasedCollapseToRange()
    {
        // With a tiny max-size, a multi-value domain must collapse to its [min, max]
        // range when finalized. This exercises the post-Phase-2 completion path.
        RuntimeStats runtimeStats = new RuntimeStats();

        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                DEFAULT_TIMEOUT,
                1L, // force range fallback
                new DynamicFilterServiceStats(),
                runtimeStats,
                false);
        filter.setExpectedPartitions(1);

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L, 30L)))));

        assertTrue(filter.isComplete(),
                "Range-fallback merge with finalized contributions must complete");

        TupleDomain<String> constraint = filter.getCurrentConstraintByColumnName();
        Domain domain = constraint.getDomains().get().get("col_a");
        assertEquals(domain.getValues().getRanges().getRangeCount(), 1,
                "Discrete values must collapse to a single range");

        assertTrue(runtimeStats.getMetrics().containsKey(DYNAMIC_FILTER_COORDINATOR_FALLBACK_TO_RANGE));
        assertEquals(runtimeStats.getMetrics().get(DYNAMIC_FILTER_COORDINATOR_FALLBACK_TO_RANGE).getSum(), 1);
    }

    @Test
    public void testSizeBasedCollapseEmitsPerFilterMetric()
    {
        RuntimeStats runtimeStats = new RuntimeStats();

        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                DEFAULT_TIMEOUT,
                1L,
                new DynamicFilterServiceStats(),
                runtimeStats,
                false);
        filter.setExpectedPartitions(1);

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L, 30L)))));

        assertTrue(filter.isComplete());

        String perFilterKey = format("%s[%s]", DYNAMIC_FILTER_COORDINATOR_FALLBACK_TO_RANGE, "549");
        assertTrue(runtimeStats.getMetrics().containsKey(perFilterKey),
                "Per-filter fallback-to-range metric should be emitted");
        assertEquals(runtimeStats.getMetrics().get(perFilterKey).getSum(), 1);
    }

    @Test
    public void testSizeBasedCollapseInSetExpectedPartitions()
    {
        RuntimeStats runtimeStats = new RuntimeStats();

        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                DEFAULT_TIMEOUT,
                1L,
                new DynamicFilterServiceStats(),
                runtimeStats,
                false);

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L)))));
        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.multipleValues(INTEGER, ImmutableList.of(30L, 40L)))));

        filter.setExpectedPartitions(2);

        assertTrue(filter.isComplete(),
                "setExpectedPartitions should latch when all tasks already finalized");
        assertTrue(runtimeStats.getMetrics().containsKey(DYNAMIC_FILTER_COORDINATOR_FALLBACK_TO_RANGE));
    }

    @Test
    public void testCollapseToRange()
    {
        TupleDomain<String> multi = TupleDomain.withColumnDomains(
                ImmutableMap.of("col", Domain.multipleValues(INTEGER, ImmutableList.of(10L, 30L, 50L))));
        TupleDomain<String> collapsed = JoinDynamicFilter.collapseToRange(multi);
        Domain domain = collapsed.getDomains().get().get("col");
        assertEquals(domain.getValues().getRanges().getRangeCount(), 1);
        assertTrue(domain.includesNullableValue(10L));
        assertTrue(domain.includesNullableValue(30L));
        assertTrue(domain.includesNullableValue(50L));
        assertTrue(domain.includesNullableValue(40L), "Range collapse must span unseen values between min and max");

        TupleDomain<String> single = TupleDomain.withColumnDomains(
                ImmutableMap.of("col", Domain.singleValue(INTEGER, 7L)));
        assertEquals(JoinDynamicFilter.collapseToRange(single), single);

        assertEquals(JoinDynamicFilter.collapseToRange(TupleDomain.none()), TupleDomain.none());
        assertEquals(JoinDynamicFilter.collapseToRange(TupleDomain.all()), TupleDomain.all());
    }

    @Test
    public void testPartitionedJoinRequiresAllTasksFinalized()
    {
        // expectedPartitions=3: completion only when 3 distinct tasks have finalized.
        // Modeling a HASH-distributed join with 3 build workers.
        RuntimeStats runtimeStats = new RuntimeStats();
        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549", "col_a", DEFAULT_TIMEOUT, DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterServiceStats(), runtimeStats, false);
        filter.setExpectedPartitions(3);

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 10L))));
        assertFalse(filter.isComplete(), "1 of 3 finalized — not complete");

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 20L))));
        assertFalse(filter.isComplete(), "2 of 3 finalized — not complete");

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 30L))));
        assertTrue(filter.isComplete(), "3 of 3 finalized — complete");

        assertEquals(filter.getCurrentConstraintByColumnName(),
                TupleDomain.withColumnDomains(ImmutableMap.of("col_a",
                        Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L, 30L)))));
    }

    @Test
    public void testRangeFallbackCompletesOnAllContributionsReceived()
    {
        // Q21 wrong-results regression: expectedPartitions=2 with both tasks
        // contributing — the merge falls back to range. Final constraint is
        // the union of both contributions, collapsed to a single [min,max] span.
        RuntimeStats runtimeStats = new RuntimeStats();
        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549", "col_a", DEFAULT_TIMEOUT, 1L /* force range */,
                new DynamicFilterServiceStats(), runtimeStats, false);
        filter.setExpectedPartitions(2);

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(ImmutableMap.of("549",
                        Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L, 30L)))));
        assertFalse(filter.isComplete(), "1 of 2 received");

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(ImmutableMap.of("549",
                        Domain.multipleValues(INTEGER, ImmutableList.of(50L, 60L, 70L)))));
        assertTrue(filter.isComplete());

        TupleDomain<String> constraint = filter.getCurrentConstraintByColumnName();
        assertFalse(constraint.isAll());
        Domain domain = constraint.getDomains().get().get("col_a");
        assertEquals(domain.getValues().getRanges().getRangeCount(), 1);
        assertTrue(domain.includesNullableValue(10L));
        assertTrue(domain.includesNullableValue(70L));
        assertTrue(domain.includesNullableValue(40L), "Range collapse spans 10-70");

        assertTrue(runtimeStats.getMetrics().containsKey(DYNAMIC_FILTER_COORDINATOR_FALLBACK_TO_RANGE));
    }

    @Test
    public void testLateFinalizationAfterFutureCompleteIsNoOp()
    {
        RuntimeStats runtimeStats = new RuntimeStats();
        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549", "col_a", DEFAULT_TIMEOUT, DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterServiceStats(), runtimeStats, false);
        filter.setExpectedPartitions(1);

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(ImmutableMap.of("549",
                        Domain.singleValue(INTEGER, 42L))));
        assertTrue(filter.isComplete());

        TupleDomain<String> resolvedConstraint = filter.getCurrentConstraintByColumnName();

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(ImmutableMap.of("549",
                        Domain.singleValue(INTEGER, 100L))));

        assertEquals(filter.getCurrentConstraintByColumnName(), resolvedConstraint);
    }

    // BEGIN ADAPTIVE-WAIT TESTS

    /**
     * Positive: contributions arrive across two cycles; the first cycle's tick sees
     * progress (1 -> 2) and extends. The final contribution then resolves the filter
     * via {@code addPartitionByFilterId} before the extension window expires.
     */
    @Test
    public void testAdaptiveExtensionAllowsLateArrivalToResolve()
    {
        RuntimeStats runtimeStats = new RuntimeStats();
        ManualScheduler scheduler = new ManualScheduler();
        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                new Duration(500, TimeUnit.MILLISECONDS),
                2,  // maxWaitExtensions
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterServiceStats(),
                runtimeStats,
                true,
                scheduler);
        filter.setExpectedPartitions(3);

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 10L))));
        filter.startTimeout();

        // Add a second contribution so the tick observes progress and extends.
        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 20L))));

        // First tick — sees progress (1→2 vs. baseline 1), grants one extension.
        scheduler.tick();
        assertFalse(filter.isComplete(), "Should not be complete yet — extension granted");

        // Third contribution arrives; tryCompleteResolution resolves synchronously.
        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 30L))));

        assertTrue(filter.isComplete(), "Filter should resolve after late arrival within extension window");
        assertFalse(runtimeStats.getMetrics().containsKey(format(DYNAMIC_FILTER_TIMED_OUT_TEMPLATE, "549")),
                "Filter resolved before timeout; no timed-out metric expected");
    }

    /**
     * Negative: a contribution arrived before startTimeout, then no further progress.
     * lastTickPartitionCount is baselined at startTimeout, so the first tick sees
     * zero new progress and finalizes immediately without consuming any extension.
     */
    @Test
    public void testNoProgressFinalizesAtFirstCycle()
    {
        RuntimeStats runtimeStats = new RuntimeStats();
        ManualScheduler scheduler = new ManualScheduler();
        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                new Duration(500, TimeUnit.MILLISECONDS),
                3,  // maxWaitExtensions — should NOT be consumed when no progress
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterServiceStats(),
                runtimeStats,
                true,
                scheduler);
        filter.setExpectedPartitions(3);

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 10L))));
        filter.startTimeout();

        // First tick observes zero new progress (baseline was set at startTimeout) — finalizes.
        scheduler.tick();

        assertFalse(filter.isComplete(), "Should not resolve without enough contributions");
        assertEquals(filter.getCurrentConstraintByColumnName(), TupleDomain.all(),
                "Partial data must not be exposed after timeout");
        assertTrue(runtimeStats.getMetrics().containsKey(format(DYNAMIC_FILTER_TIMED_OUT_TEMPLATE, "549")),
                "Filter must be marked timed out");
    }

    /**
     * Cap: contributions trickle in every tick but never reach the expected count.
     * The filter must finalize after exactly (1 + maxWaitExtensions) ticks.
     */
    @Test
    public void testExtensionsCappedByMaxWaitExtensions()
    {
        RuntimeStats runtimeStats = new RuntimeStats();
        ManualScheduler scheduler = new ManualScheduler();
        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                new Duration(500, TimeUnit.MILLISECONDS),
                2,  // maxWaitExtensions
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterServiceStats(),
                runtimeStats,
                true,
                scheduler);
        filter.setExpectedPartitions(100);
        filter.startTimeout();

        // Trickle one contribution per tick; after (1 + 2) = 3 ticks the cap fires.
        for (int i = 0; i < 3; i++) {
            filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                    ImmutableMap.of("549", Domain.singleValue(INTEGER, (long) i))));
            scheduler.tick();
        }

        assertFalse(filter.isComplete(), "Cap should fire before expectedPartitions reached");
        assertEquals(filter.getCurrentConstraintByColumnName(), TupleDomain.all(),
                "Partial data must not be exposed after capped timeout");
    }

    /**
     * Correctness: with adaptive extension enabled, the future is completed with
     * all() on timeout and isComplete() / getCurrentConstraintByColumnName() preserve
     * the all-or-nothing contract (no partial constraint exposed).
     */
    @Test
    public void testAdaptiveTimeoutPreservesAllOrNothingContract()
    {
        RuntimeStats runtimeStats = new RuntimeStats();
        ManualScheduler scheduler = new ManualScheduler();
        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                new Duration(500, TimeUnit.MILLISECONDS),
                3,  // maxWaitExtensions
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterServiceStats(),
                runtimeStats,
                false,  // extendedMetrics off — exercise the non-diagnostic path
                scheduler);
        filter.setExpectedPartitions(4);

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 10L))));
        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 20L))));
        filter.startTimeout();

        // No further contributions — tick through all extensions to exhaust the cap.
        // Each tick sees no new progress after the first one (count stays at 2).
        scheduler.tick(); // tick 1: progress baseline was 2, current is 2 → no progress → finalize
        // (With maxWaitExtensions=3 and no progress, the first tick should finalize immediately)

        assertFalse(filter.isComplete(),
                "Filter must remain incomplete after timeout despite partial contributions");
        assertEquals(filter.getCurrentConstraintByColumnName(), TupleDomain.all(),
                "Partial union (10, 20) must not be exposed to connector");
        assertTrue(filter.isBlocked().isDone(),
                "Future must be completed so the connector unblocks");

        // Even a super-late arrival after the future completed must not flip fullyResolved.
        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 30L))));
        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.singleValue(INTEGER, 40L))));
        assertFalse(filter.isComplete(),
                "Late arrivals after timeout must not resolve the filter");
        assertEquals(filter.getCurrentConstraintByColumnName(), TupleDomain.all());
    }
    // END ADAPTIVE-WAIT TESTS

    /**
     * A deterministic {@link ScheduledExecutorService} for tests: captures the most
     * recently scheduled runnable and fires it synchronously on {@link #tick()}.
     * Replaces {@code Thread.sleep} in adaptive-wait tests.
     */
    private static final class ManualScheduler
            implements ScheduledExecutorService
    {
        private final AtomicReference<Runnable> pending = new AtomicReference<>();
        private final ScheduledExecutorService delegate = Executors.newSingleThreadScheduledExecutor();

        /** Fire the most recently scheduled runnable synchronously. */
        public void tick()
        {
            Runnable task = pending.getAndSet(null);
            if (task != null) {
                task.run();
            }
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
        {
            pending.set(command);
            return delegate.schedule(() -> {}, 0, TimeUnit.MILLISECONDS);
        }

        @Override
        public <V> ScheduledFuture<V> schedule(java.util.concurrent.Callable<V> callable, long delay, TimeUnit unit)
        {
            return delegate.schedule(callable, 0, TimeUnit.MILLISECONDS);
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit)
        {
            return delegate.scheduleAtFixedRate(command, 0, period, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit)
        {
            return delegate.scheduleWithFixedDelay(command, 0, delay, unit);
        }

        @Override
        public void shutdown()
        {
            delegate.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow()
        {
            return delegate.shutdownNow();
        }

        @Override
        public boolean isShutdown()
        {
            return delegate.isShutdown();
        }

        @Override
        public boolean isTerminated()
        {
            return delegate.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit)
                throws InterruptedException
        {
            return delegate.awaitTermination(timeout, unit);
        }

        @Override
        public <T> Future<T> submit(Callable<T> task)
        {
            return delegate.submit(task);
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result)
        {
            return delegate.submit(task, result);
        }

        @Override
        public Future<?> submit(Runnable task)
        {
            return delegate.submit(task);
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
                throws InterruptedException
        {
            return delegate.invokeAll(tasks);
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
                throws InterruptedException
        {
            return delegate.invokeAll(tasks, timeout, unit);
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
                throws InterruptedException, ExecutionException
        {
            return delegate.invokeAny(tasks);
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException
        {
            return delegate.invokeAny(tasks, timeout, unit);
        }

        @Override
        public void execute(Runnable command)
        {
            delegate.execute(command);
        }
    }
}
