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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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
    public static final String DYNAMIC_FILTER_PARTITIONS_RECEIVED_TEMPLATE = DYNAMIC_FILTER_PARTITIONS_RECEIVED + "[%s]";
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
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterStats(),
                runtimeStats,
                false);
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
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterStats(),
                runtimeStats,
                false);
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
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterStats(),
                runtimeStats,
                false);
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
        JoinDynamicFilter defaultFilter = new JoinDynamicFilter(
                "",
                "",
                DEFAULT_TIMEOUT,
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterStats(),
                new RuntimeStats(),
                false);
        assertEquals(defaultFilter.getFilterId(), "");

        RuntimeStats runtimeStats = new RuntimeStats();
        JoinDynamicFilter namedFilter = new JoinDynamicFilter(
                "549",
                "column_a",
                DEFAULT_TIMEOUT,
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterStats(),
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
                new DynamicFilterStats(),
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
                new DynamicFilterStats(),
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
            throws Exception
    {
        RuntimeStats runtimeStats = new RuntimeStats();
        DynamicFilterStats stats = new DynamicFilterStats();

        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                new Duration(100, TimeUnit.MILLISECONDS),
                DEFAULT_MAX_SIZE_BYTES,
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
                DEFAULT_MAX_SIZE_BYTES,
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
                DEFAULT_MAX_SIZE_BYTES,
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

    @Test
    public void testSizeBasedCollapseToRange()
    {
        RuntimeStats runtimeStats = new RuntimeStats();

        // Use a very small max size (1 byte) to force collapse on any non-trivial domain
        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                DEFAULT_TIMEOUT,
                1L, // maxSizeInBytes — 1 byte forces collapse
                new DynamicFilterStats(),
                runtimeStats,
                false);
        filter.setExpectedPartitions(1);

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L, 30L)))));

        assertTrue(filter.isComplete());

        TupleDomain<String> constraint = filter.getCurrentConstraintByColumnName();
        assertFalse(constraint.isAll());
        assertFalse(constraint.isNone());

        Domain domain = constraint.getDomains().get().get("col_a");
        assertEquals(domain.getValues().getRanges().getRangeCount(), 1);

        assertTrue(domain.includesNullableValue(10L));
        assertTrue(domain.includesNullableValue(20L));
        assertTrue(domain.includesNullableValue(30L));

        assertTrue(runtimeStats.getMetrics().containsKey(DYNAMIC_FILTER_COORDINATOR_FALLBACK_TO_RANGE),
                "Aggregate fallback-to-range metric should be present");
    }

    @Test
    public void testSizeBasedCollapseEmitsPerFilterMetric()
    {
        RuntimeStats runtimeStats = new RuntimeStats();

        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                DEFAULT_TIMEOUT,
                1L, // force collapse
                new DynamicFilterStats(),
                runtimeStats,
                false);
        filter.setExpectedPartitions(1);

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L, 30L)))));

        assertTrue(filter.isComplete());

        String perFilterFallbackKey = format("%s[%s]", DYNAMIC_FILTER_COORDINATOR_FALLBACK_TO_RANGE, "549");
        assertTrue(runtimeStats.getMetrics().containsKey(perFilterFallbackKey),
                "Per-filter fallback-to-range metric should be present");
        assertEquals(runtimeStats.getMetrics().get(perFilterFallbackKey).getSum(), 1);
    }

    @Test
    public void testNoCollapseWhenUnderSizeLimit()
    {
        RuntimeStats runtimeStats = new RuntimeStats();

        // Use a large max size (1MB) — small domains should not be collapsed
        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                DEFAULT_TIMEOUT,
                1_048_576L, // 1MB
                new DynamicFilterStats(),
                runtimeStats,
                false);
        filter.setExpectedPartitions(1);

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L, 30L)))));

        assertTrue(filter.isComplete());

        TupleDomain<String> constraint = filter.getCurrentConstraintByColumnName();
        Domain domain = constraint.getDomains().get().get("col_a");

        // Should keep all 3 discrete values (not collapsed)
        assertEquals(domain.getValues().getRanges().getRangeCount(), 3);

        // Verify no fallback metric
        assertFalse(runtimeStats.getMetrics().containsKey(DYNAMIC_FILTER_COORDINATOR_FALLBACK_TO_RANGE),
                "Fallback metric should not be emitted when under size limit");
    }

    @Test
    public void testSizeBasedCollapseInSetExpectedPartitions()
    {
        RuntimeStats runtimeStats = new RuntimeStats();

        // Use 1 byte max to force collapse
        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                DEFAULT_TIMEOUT,
                1L,
                new DynamicFilterStats(),
                runtimeStats,
                false);

        // Add partitions before setting expected count
        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L)))));
        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.multipleValues(INTEGER, ImmutableList.of(30L, 40L)))));
        assertFalse(filter.isComplete());

        // Setting expected = 2 triggers merge, which should trigger collapse
        filter.setExpectedPartitions(2);
        assertTrue(filter.isComplete());

        TupleDomain<String> constraint = filter.getCurrentConstraintByColumnName();
        Domain domain = constraint.getDomains().get().get("col_a");
        // Collapsed to single range spanning 10-40
        assertEquals(domain.getValues().getRanges().getRangeCount(), 1);
        assertTrue(domain.includesNullableValue(10L));
        assertTrue(domain.includesNullableValue(40L));
    }

    @Test
    public void testEstimateRetainedSizeInBytes()
    {
        // none/all have zero size
        assertEquals(JoinDynamicFilter.estimateRetainedSizeInBytes(TupleDomain.none()), 0);
        assertEquals(JoinDynamicFilter.estimateRetainedSizeInBytes(TupleDomain.all()), 0);

        // Single value has non-zero size (block storage for low + high markers)
        TupleDomain<String> singleValue = TupleDomain.withColumnDomains(
                ImmutableMap.of("col", Domain.singleValue(INTEGER, 10L)));
        assertTrue(JoinDynamicFilter.estimateRetainedSizeInBytes(singleValue) > 0);

        // More values = more size
        TupleDomain<String> multiValue = TupleDomain.withColumnDomains(
                ImmutableMap.of("col", Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L, 30L))));
        assertTrue(JoinDynamicFilter.estimateRetainedSizeInBytes(multiValue) >
                JoinDynamicFilter.estimateRetainedSizeInBytes(singleValue));
    }

    @Test
    public void testCollapseToRange()
    {
        // Single value — no collapse needed
        TupleDomain<String> singleValue = TupleDomain.withColumnDomains(
                ImmutableMap.of("col", Domain.singleValue(INTEGER, 10L)));
        TupleDomain<String> collapsed = JoinDynamicFilter.collapseToRange(singleValue);
        assertEquals(collapsed, singleValue);

        // Multiple values — collapse to span
        TupleDomain<String> multiValue = TupleDomain.withColumnDomains(
                ImmutableMap.of("col", Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L, 30L))));
        collapsed = JoinDynamicFilter.collapseToRange(multiValue);
        Domain domain = collapsed.getDomains().get().get("col");
        assertEquals(domain.getValues().getRanges().getRangeCount(), 1);
        assertTrue(domain.includesNullableValue(10L));
        assertTrue(domain.includesNullableValue(15L)); // intermediate value included in range
        assertTrue(domain.includesNullableValue(30L));

        // none() and all() pass through unchanged
        assertEquals(JoinDynamicFilter.collapseToRange(TupleDomain.none()), TupleDomain.none());
        assertEquals(JoinDynamicFilter.collapseToRange(TupleDomain.all()), TupleDomain.all());
    }

    @Test
    public void testNoCollapseWithDefaultSize()
    {
        RuntimeStats runtimeStats = new RuntimeStats();

        // Default max size (1 MB) — small domains should not be collapsed
        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                DEFAULT_TIMEOUT,
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterStats(),
                runtimeStats,
                false);
        filter.setExpectedPartitions(1);

        // Build a domain with many values — still well under 1 MB
        List<Long> values = new ArrayList<>();
        for (long i = 0; i < 100; i++) {
            values.add(i);
        }
        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.multipleValues(INTEGER, values))));

        assertTrue(filter.isComplete());

        TupleDomain<String> constraint = filter.getCurrentConstraintByColumnName();
        Domain domain = constraint.getDomains().get().get("col_a");
        // Should preserve all 100 discrete values (well under 1 MB limit)
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
                new DynamicFilterStats(),
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
                new DynamicFilterStats(),
                runtimeStats,
                false);

        filter.setProbeColumnDomain(Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)));
        filter.setExpectedPartitions(1);

        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)))));

        assertTrue(filter.isComplete());
        assertEquals(filter.getCurrentConstraintByColumnName(), TupleDomain.all());

        // Per-filter short-circuit metric
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
                new DynamicFilterStats(),
                runtimeStats,
                false);

        // Probe column domain: values [1, 2, 3, 4, 5]
        filter.setProbeColumnDomain(Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L, 4L, 5L)));
        filter.setExpectedPartitions(1);

        // Build side only covers subset [1, 2, 3] — cannot short-circuit
        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)))));

        assertTrue(filter.isComplete());

        // No short-circuit: constraint preserved
        TupleDomain<String> constraint = filter.getCurrentConstraintByColumnName();
        assertFalse(constraint.isAll(),
                "Filter should NOT short-circuit when build does not cover probe");
        assertEquals(
                constraint,
                TupleDomain.withColumnDomains(
                        ImmutableMap.of("col_a", Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)))));

        // No short-circuit metric
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
                new DynamicFilterStats(),
                runtimeStats,
                false);

        // No probeColumnDomain set — should never short-circuit
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
        // Test the setExpectedPartitions completion path (add partitions first, then set expected)
        RuntimeStats runtimeStats = new RuntimeStats();

        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                DEFAULT_TIMEOUT,
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterStats(),
                runtimeStats,
                false);

        filter.setProbeColumnDomain(Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L)));

        // Add partitions before setting expected count
        filter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of("549", Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)))));
        assertFalse(filter.isComplete());

        // Setting expected = 1 triggers completion through setExpectedPartitions path
        filter.setExpectedPartitions(1);
        assertTrue(filter.isComplete());

        // Short-circuit should have fired in setExpectedPartitions path
        assertEquals(filter.getCurrentConstraintByColumnName(), TupleDomain.all(),
                "Short-circuit should fire via setExpectedPartitions path");

        assertTrue(runtimeStats.getMetrics().containsKey(DYNAMIC_FILTER_SHORT_CIRCUITED));
    }

    @Test
    public void testShortCircuitWithExactMatch()
    {
        // Build domain exactly matches probe domain — should short-circuit
        RuntimeStats runtimeStats = new RuntimeStats();

        JoinDynamicFilter filter = new JoinDynamicFilter(
                "549",
                "col_a",
                DEFAULT_TIMEOUT,
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterStats(),
                runtimeStats,
                false);

        Domain probeDomain = Domain.multipleValues(INTEGER, ImmutableList.of(10L, 20L, 30L));
        filter.setProbeColumnDomain(probeDomain);
        filter.setExpectedPartitions(1);

        // Build side collects exactly the same values
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
                new DynamicFilterStats(),
                runtimeStats,
                false);

        filter.setProbeColumnDomain(Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L)));
        filter.setExpectedPartitions(1);

        // Build side produces none() (empty build)
        filter.addPartitionByFilterId(TupleDomain.none());

        assertTrue(filter.isComplete());

        // none() should NOT be short-circuited — it means the build was empty and
        // we should prune everything
        TupleDomain<String> constraint = filter.getCurrentConstraintByColumnName();
        assertTrue(constraint.isNone(),
                "none() constraint should be preserved (empty build prunes everything)");
        assertFalse(runtimeStats.getMetrics().containsKey(DYNAMIC_FILTER_SHORT_CIRCUITED));
    }
}
