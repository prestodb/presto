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
package com.facebook.presto.common;

import com.facebook.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static com.facebook.presto.common.RuntimeUnit.BYTE;
import static com.facebook.presto.common.RuntimeUnit.NANO;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestRuntimeMetric
{
    private static final String TEST_METRIC_NAME = "test_metric";

    private void assertRuntimeMetricEquals(RuntimeMetric m1, RuntimeMetric m2)
    {
        assertEquals(m1.getName(), m2.getName());
        assertEquals(m1.getUnit(), m2.getUnit());
        assertEquals(m1.getSum(), m2.getSum());
        assertEquals(m1.getCount(), m2.getCount());
        assertEquals(m1.getMax(), m2.getMax());
        assertEquals(m1.getMin(), m2.getMin());
    }

    @Test
    public void testAddValue()
    {
        RuntimeMetric metric = new RuntimeMetric(TEST_METRIC_NAME, NONE);
        metric.addValue(101);
        assertRuntimeMetricEquals(metric, new RuntimeMetric(TEST_METRIC_NAME, NONE, 101, 1, 101, 101));

        metric.addValue(99);
        assertRuntimeMetricEquals(metric, new RuntimeMetric(TEST_METRIC_NAME, NONE, 200, 2, 101, 99));

        metric.addValue(201);
        assertRuntimeMetricEquals(metric, new RuntimeMetric(TEST_METRIC_NAME, NONE, 401, 3, 201, 99));

        metric.addValue(202);
        assertRuntimeMetricEquals(metric, new RuntimeMetric(TEST_METRIC_NAME, NONE, 603, 4, 202, 99));
    }

    @Test
    public void testCopy()
    {
        RuntimeMetric metric = new RuntimeMetric(TEST_METRIC_NAME, NONE, 1, 1, 1, 1);
        RuntimeMetric metricCopy = RuntimeMetric.copyOf(metric);

        // Verify that updating one metric doesn't affect its copy.
        metric.addValue(2);
        assertRuntimeMetricEquals(metric, new RuntimeMetric(TEST_METRIC_NAME, NONE, 3, 2, 2, 1));
        assertRuntimeMetricEquals(metricCopy, new RuntimeMetric(TEST_METRIC_NAME, NONE, 1, 1, 1, 1));
        metricCopy.addValue(2);
        assertRuntimeMetricEquals(metric, new RuntimeMetric(TEST_METRIC_NAME, NONE, 3, 2, 2, 1));
        assertRuntimeMetricEquals(metricCopy, metric);
    }

    @Test
    public void testMergeWith()
    {
        RuntimeMetric metric1 = new RuntimeMetric(TEST_METRIC_NAME, NONE, 5, 2, 4, 1);
        RuntimeMetric metric2 = new RuntimeMetric(TEST_METRIC_NAME, NONE, 20, 2, 11, 9);
        metric1.mergeWith(metric2);
        assertRuntimeMetricEquals(metric1, new RuntimeMetric(TEST_METRIC_NAME, NONE, 25, 4, 11, 1));

        metric2.mergeWith(metric2);
        assertRuntimeMetricEquals(metric2, new RuntimeMetric(TEST_METRIC_NAME, NONE, 40, 4, 11, 9));

        metric2.mergeWith(null);
        assertRuntimeMetricEquals(metric2, new RuntimeMetric(TEST_METRIC_NAME, NONE, 40, 4, 11, 9));
    }

    @Test(expectedExceptions = {IllegalStateException.class})
    public void testMergeWithWithConflictUnits()
    {
        RuntimeMetric metric1 = new RuntimeMetric(TEST_METRIC_NAME, NANO, 5, 2, 4, 1);
        RuntimeMetric metric2 = new RuntimeMetric(TEST_METRIC_NAME, BYTE, 20, 2, 11, 9);
        metric1.mergeWith(metric2);
    }

    @Test
    public void testMerge()
    {
        RuntimeMetric metric1 = new RuntimeMetric(TEST_METRIC_NAME, NONE, 5, 2, 4, 1);
        RuntimeMetric metric2 = new RuntimeMetric(TEST_METRIC_NAME, NONE, 20, 2, 11, 9);
        assertRuntimeMetricEquals(RuntimeMetric.merge(metric1, metric2), new RuntimeMetric(TEST_METRIC_NAME, NONE, 25, 4, 11, 1));

        assertRuntimeMetricEquals(metric1, new RuntimeMetric(TEST_METRIC_NAME, NONE, 5, 2, 4, 1));
        assertRuntimeMetricEquals(metric2, new RuntimeMetric(TEST_METRIC_NAME, NONE, 20, 2, 11, 9));
    }

    @Test(expectedExceptions = {IllegalStateException.class})
    public void testMergeWithConflictUnits()
    {
        RuntimeMetric metric1 = new RuntimeMetric(TEST_METRIC_NAME, NANO, 5, 2, 4, 1);
        RuntimeMetric metric2 = new RuntimeMetric(TEST_METRIC_NAME, BYTE, 20, 2, 11, 9);
        RuntimeMetric.merge(metric1, metric2);
    }

    @Test
    public void testJson()
    {
        JsonCodec<RuntimeMetric> codec = JsonCodec.jsonCodec(RuntimeMetric.class);
        RuntimeMetric metric = new RuntimeMetric(TEST_METRIC_NAME, NANO);
        metric.addValue(101);
        metric.addValue(202);

        String json = codec.toJson(metric);
        RuntimeMetric actual = codec.fromJson(json);

        assertRuntimeMetricEquals(actual, metric);
    }

    @Test
    public void testJsonWhenUnitIsUnavailable()
    {
        RuntimeMetric metric1 = new RuntimeMetric(TEST_METRIC_NAME, NONE);
        metric1.addValue(101);
        metric1.addValue(202);
        RuntimeMetric metric2 = new RuntimeMetric(TEST_METRIC_NAME, null);
        metric2.addValue(202);
        metric2.addValue(101);

        String json = "{\"name\" : \"test_metric\", \"sum\" : 303, \"count\" : 2, \"max\" : 202, \"min\" : 101}";
        RuntimeMetric actual = JsonCodec.jsonCodec(RuntimeMetric.class).fromJson(json);
        assertRuntimeMetricEquals(actual, metric1);
        assertRuntimeMetricEquals(actual, metric2);
    }

    @Test
    public void testPercentilesDisabledByDefault()
    {
        RuntimeMetric metric = new RuntimeMetric(TEST_METRIC_NAME, NANO);
        metric.addValue(100);
        metric.addValue(200);
        metric.addValue(300);

        assertEquals(metric.isPercentileTrackingEnabled(), false);
        assertEquals(metric.getP90(), null);
        assertEquals(metric.getP95(), null);
        assertEquals(metric.getP99(), null);
    }

    @Test
    public void testPercentilesWithTracking()
    {
        RuntimeMetric metric = new RuntimeMetric(TEST_METRIC_NAME, NANO, true);

        // Add 100 values from 0 to 99ms in nanoseconds
        for (int i = 0; i < 100; i++) {
            metric.addValue(i * 1_000_000); // Convert to nanoseconds
        }

        assertEquals(metric.isPercentileTrackingEnabled(), true);
        assertEquals(metric.getCount(), 100);
        assertEquals(metric.getSum(), 4_950_000_000L); // Sum in nanoseconds

        // Verify percentiles are computed
        Long p90 = metric.getP90();
        Long p95 = metric.getP95();
        Long p99 = metric.getP99();

        assertEquals(p90 != null, true);
        assertEquals(p95 != null, true);
        assertEquals(p99 != null, true);

        // Verify percentile values are in reasonable range (approximate due to bucketing)
        assertEquals(p90 >= 88_000_000 && p90 <= 92_000_000, true);
        assertEquals(p95 >= 93_000_000 && p95 <= 97_000_000, true);
        assertEquals(p99 >= 97_000_000 && p99 <= 100_000_000, true);
    }

    @Test
    public void testPercentileJsonSerialization()
    {
        RuntimeMetric metric = new RuntimeMetric(TEST_METRIC_NAME, NANO, true);

        // Add values with percentile tracking (in nanoseconds)
        for (int i = 0; i < 50; i++) {
            metric.addValue(i * 1_000_000);
        }

        // Serialize to JSON
        JsonCodec<RuntimeMetric> codec = JsonCodec.jsonCodec(RuntimeMetric.class);
        String json = codec.toJson(metric);

        // Verify JSON contains percentile values
        assertEquals(json.contains("\"p90\""), true);
        assertEquals(json.contains("\"p95\""), true);
        assertEquals(json.contains("\"p99\""), true);

        // Deserialize and verify percentiles are preserved
        RuntimeMetric deserialized = codec.fromJson(json);
        assertEquals(deserialized.isPercentileTrackingEnabled(), false);
        assertEquals(deserialized.getP90(), metric.getP90());
        assertEquals(deserialized.getP95(), metric.getP95());
        assertEquals(deserialized.getP99(), metric.getP99());
    }

    @Test
    public void testPercentileCopy()
    {
        RuntimeMetric metric = new RuntimeMetric(TEST_METRIC_NAME, NANO, true);

        // Add values with percentile tracking (in nanoseconds)
        for (int i = 0; i < 50; i++) {
            metric.addValue(i * 1_000_000);
        }

        RuntimeMetric copy = RuntimeMetric.copyOf(metric);

        // Verify percentiles are copied
        assertEquals(copy.isPercentileTrackingEnabled(), true);
        assertEquals(copy.getP90(), metric.getP90());
        assertEquals(copy.getP95(), metric.getP95());
        assertEquals(copy.getP99(), metric.getP99());

        // Verify copy is independent
        metric.addValue(100_000_000);
        assertEquals(copy.getCount() != metric.getCount(), true);
    }

    @Test
    public void testPercentileMerge()
    {
        RuntimeMetric metric1 = new RuntimeMetric(TEST_METRIC_NAME, NANO, true);
        RuntimeMetric metric2 = new RuntimeMetric(TEST_METRIC_NAME, NANO, true);

        // Add values with percentile tracking to both metrics (in nanoseconds)
        for (int i = 0; i < 50; i++) {
            metric1.addValue(i * 1_000_000);
        }
        for (int i = 50; i < 100; i++) {
            metric2.addValue(i * 1_000_000);
        }

        RuntimeMetric merged = RuntimeMetric.merge(metric1, metric2);

        // Verify merged metrics have correct count
        assertEquals(merged.getCount(), 100);
        assertEquals(merged.isPercentileTrackingEnabled(), true);

        // Verify percentiles are recomputed after merge
        Long p90 = merged.getP90();
        assertEquals(p90 != null, true);
        assertEquals(p90 >= 88_000_000 && p90 <= 92_000_000, true);
    }

    @Test
    public void testPercentileMergeWith()
    {
        RuntimeMetric metric1 = new RuntimeMetric(TEST_METRIC_NAME, NANO, true);
        RuntimeMetric metric2 = new RuntimeMetric(TEST_METRIC_NAME, NANO, true);

        // Add values with percentile tracking (in nanoseconds)
        for (int i = 0; i < 50; i++) {
            metric1.addValue(i * 1_000_000);
        }
        for (int i = 50; i < 100; i++) {
            metric2.addValue(i * 1_000_000);
        }

        metric1.mergeWith(metric2);

        // Verify merged count
        assertEquals(metric1.getCount(), 100);
        assertEquals(metric1.isPercentileTrackingEnabled(), true);

        // Verify percentiles are available after merge
        assertEquals(metric1.getP90() != null, true);
        assertEquals(metric1.getP95() != null, true);
        assertEquals(metric1.getP99() != null, true);
    }

    @Test
    public void testByteUnitWithDefaultBucketWidth()
    {
        // BYTE unit defaults to 1KB (1024 bytes) bucket width
        RuntimeMetric metric = new RuntimeMetric("data_size", RuntimeUnit.BYTE, true);

        // Add 100 values from 0 to 99KB (in bytes)
        for (int i = 0; i < 100; i++) {
            metric.addValue(i * 1024);
        }

        assertEquals(metric.getCount(), 100);
        assertEquals(metric.getMin(), 0);
        assertEquals(metric.getMax(), 99 * 1024);
        assertTrue(metric.isPercentileTrackingEnabled());

        // With 1KB buckets, precision should be ±512 bytes
        Long p90 = metric.getP90();
        assertTrue(p90 != null);
        assertTrue(p90 >= 88 * 1024 && p90 <= 92 * 1024, "p90 was " + p90 + " bytes");

        Long p95 = metric.getP95();
        assertTrue(p95 != null);
        assertTrue(p95 >= 93 * 1024 && p95 <= 97 * 1024, "p95 was " + p95 + " bytes");
    }

    @Test
    public void testByteUnitWithCustomBucketWidth()
    {
        // Custom: 100 bytes per bucket for small file sizes
        RuntimeMetric metric = new RuntimeMetric("small_file_size", RuntimeUnit.BYTE, true, 100);

        // Add 100 values from 0 to 9900 bytes (0-9.9KB)
        for (int i = 0; i < 100; i++) {
            metric.addValue(i * 100);
        }

        assertEquals(metric.getCount(), 100);
        assertEquals(metric.getMin(), 0);
        assertEquals(metric.getMax(), 9900);
        assertTrue(metric.isPercentileTrackingEnabled());

        // With 100 byte buckets, precision should be ±50 bytes
        Long p90 = metric.getP90();
        assertTrue(p90 != null);
        assertTrue(p90 >= 8850 && p90 <= 9050, "p90 was " + p90 + " bytes (expected ~8900-9000 with ±50 bytes precision)");

        Long p99 = metric.getP99();
        assertTrue(p99 != null);
        assertTrue(p99 >= 9750 && p99 <= 10000, "p99 was " + p99 + " bytes");
    }

    @Test
    public void testNoneUnitWithDefaultBucketWidth()
    {
        // NONE unit defaults to 1000 per bucket
        RuntimeMetric metric = new RuntimeMetric("row_count", RuntimeUnit.NONE, true);

        // Add 100 values from 0 to 99,000
        for (int i = 0; i < 100; i++) {
            metric.addValue(i * 1000);
        }

        assertEquals(metric.getCount(), 100);
        assertEquals(metric.getMin(), 0);
        assertEquals(metric.getMax(), 99000);
        assertTrue(metric.isPercentileTrackingEnabled());

        // With 1000 unit buckets, precision should be ±500 units
        Long p90 = metric.getP90();
        assertTrue(p90 != null);
        assertTrue(p90 >= 88000 && p90 <= 92000, "p90 was " + p90);

        Long p95 = metric.getP95();
        assertTrue(p95 != null);
        assertTrue(p95 >= 93000 && p95 <= 97000, "p95 was " + p95);
    }

    @Test
    public void testNoneUnitWithCustomBucketWidth()
    {
        // Custom: 10,000 per bucket for large row counts
        RuntimeMetric metric = new RuntimeMetric("large_row_count", RuntimeUnit.NONE, true, 10000);

        // Add 100 values from 0 to 990,000
        for (int i = 0; i < 100; i++) {
            metric.addValue(i * 10000);
        }

        assertEquals(metric.getCount(), 100);
        assertEquals(metric.getMin(), 0);
        assertEquals(metric.getMax(), 990000);
        assertTrue(metric.isPercentileTrackingEnabled());

        // With 10,000 unit buckets, precision should be ±5,000 units
        Long p90 = metric.getP90();
        assertTrue(p90 != null);
        assertTrue(p90 >= 885000 && p90 <= 905000, "p90 was " + p90 + " (expected ~890k-900k with ±5k precision)");

        Long p99 = metric.getP99();
        assertTrue(p99 != null);
        assertTrue(p99 >= 975000 && p99 <= 1000000, "p99 was " + p99);
    }

    @Test
    public void testLargeByteCountsWithCustomBucketWidth()
    {
        // Custom: 1MB (1048576 bytes) per bucket for large data sizes
        RuntimeMetric metric = new RuntimeMetric("large_data_transfer", RuntimeUnit.BYTE, true, 1048576);

        // Add 100 values from 0 to 99MB (in bytes)
        for (int i = 0; i < 100; i++) {
            metric.addValue(i * 1048576L);
        }

        assertEquals(metric.getCount(), 100);
        assertEquals(metric.getMin(), 0);
        assertEquals(metric.getMax(), 99L * 1048576);
        assertTrue(metric.isPercentileTrackingEnabled());

        // With 1MB buckets, precision should be ±0.5MB
        Long p90 = metric.getP90();
        assertTrue(p90 != null);
        long expectedP90 = 89L * 1048576; // ~89MB
        assertTrue(Math.abs(p90 - expectedP90) <= 2 * 1048576, "p90 was " + p90 + " bytes (expected ~" + expectedP90 + " ±2MB)");
    }

    @Test
    public void testSmallRowCountsWithCustomBucketWidth()
    {
        // Custom: 10 per bucket for small row counts
        RuntimeMetric metric = new RuntimeMetric("small_batch_size", RuntimeUnit.NONE, true, 10);

        // Add 100 values from 0 to 990
        for (int i = 0; i < 100; i++) {
            metric.addValue(i * 10);
        }

        assertEquals(metric.getCount(), 100);
        assertEquals(metric.getMin(), 0);
        assertEquals(metric.getMax(), 990);
        assertTrue(metric.isPercentileTrackingEnabled());

        // With 10 unit buckets, precision should be ±5 units
        Long p90 = metric.getP90();
        assertTrue(p90 != null);
        assertTrue(p90 >= 885 && p90 <= 905, "p90 was " + p90 + " (expected ~890-900 with ±5 precision)");
    }

    @Test
    public void testDifferentUnitsComparison()
    {
        // Create metrics with different units, all tracking percentiles
        RuntimeMetric nanoMetric = new RuntimeMetric("latency", NANO, true);
        RuntimeMetric byteMetric = new RuntimeMetric("data_size", RuntimeUnit.BYTE, true);
        RuntimeMetric noneMetric = new RuntimeMetric("row_count", RuntimeUnit.NONE, true);

        // Add 100 values to each
        for (int i = 0; i < 100; i++) {
            nanoMetric.addValue(i * 1_000_000); // 0-99ms in nanoseconds
            byteMetric.addValue(i * 1024); // 0-99KB in bytes
            noneMetric.addValue(i * 1000); // 0-99k in count
        }

        // All should have percentile tracking enabled
        assertTrue(nanoMetric.isPercentileTrackingEnabled());
        assertTrue(byteMetric.isPercentileTrackingEnabled());
        assertTrue(noneMetric.isPercentileTrackingEnabled());

        // All should have valid percentiles
        assertTrue(nanoMetric.getP90() != null);
        assertTrue(byteMetric.getP90() != null);
        assertTrue(noneMetric.getP90() != null);

        // Verify each is in appropriate range for its unit
        Long nanoP90 = nanoMetric.getP90();
        assertTrue(nanoP90 >= 88_000_000 && nanoP90 <= 92_000_000, "nano p90 was " + nanoP90);

        Long byteP90 = byteMetric.getP90();
        assertTrue(byteP90 >= 88 * 1024 && byteP90 <= 92 * 1024, "byte p90 was " + byteP90);

        Long noneP90 = noneMetric.getP90();
        assertTrue(noneP90 >= 88000 && noneP90 <= 92000, "none p90 was " + noneP90);
    }

    @Test(expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = ".*must have the same unit type.*")
    public void testMergeMetricsWithDifferentUnits()
    {
        // Create two metrics with different units
        RuntimeMetric m1 = new RuntimeMetric("metric1", NANO, true);
        RuntimeMetric m2 = new RuntimeMetric("metric2", RuntimeUnit.BYTE, true);

        // Add values to both
        for (int i = 0; i < 50; i++) {
            m1.addValue(i * 1_000_000);
            m2.addValue(i * 1024);
        }

        // This should throw IllegalStateException due to different units
        m1.mergeWith(m2);
    }

    @Test(expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = ".*must have the same bucket width.*")
    public void testMergeMetricsWithDifferentBucketWidths()
    {
        // Create two metrics with same unit but different bucket widths
        RuntimeMetric m1 = new RuntimeMetric("latency", NANO, true, 1_000_000); // 1ms buckets
        RuntimeMetric m2 = new RuntimeMetric("latency", NANO, true, 10_000_000); // 10ms buckets

        // Add values to both
        for (int i = 0; i < 50; i++) {
            m1.addValue(i * 1_000_000);
            m2.addValue(i * 1_000_000);
        }

        // This should throw IllegalStateException due to different bucket widths
        m1.mergeWith(m2);
    }

    @Test(expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = ".*must have the same bucket number.*")
    public void testMergeMetricsWithDifferentNumBuckets()
    {
        // Create two metrics with same unit and bucket width but different number of buckets
        RuntimeMetric m1 = new RuntimeMetric("latency", NANO, true, 1_000_000, 1000); // 1000 buckets
        RuntimeMetric m2 = new RuntimeMetric("latency", NANO, true, 1_000_000, 500);  // 500 buckets

        // Add values to both
        for (int i = 0; i < 50; i++) {
            m1.addValue(i * 1_000_000);
            m2.addValue(i * 1_000_000);
        }

        // This should throw IllegalStateException due to different number of buckets
        m1.mergeWith(m2);
    }

    @Test
    public void testMergeMetricsWithSameBucketWidth()
    {
        // Create two metrics with same unit AND same custom bucket width
        RuntimeMetric m1 = new RuntimeMetric("latency", NANO, true, 100_000); // 100μs buckets
        RuntimeMetric m2 = new RuntimeMetric("latency", NANO, true, 100_000); // 100μs buckets

        // Add values to both
        for (int i = 0; i < 50; i++) {
            m1.addValue(i * 100_000);
        }
        for (int i = 50; i < 100; i++) {
            m2.addValue(i * 100_000);
        }

        // This should succeed
        RuntimeMetric merged = RuntimeMetric.merge(m1, m2);

        assertEquals(merged.getCount(), 100);
        assertTrue(merged.isPercentileTrackingEnabled());

        Long p90 = merged.getP90();
        assertTrue(p90 != null);
        assertTrue(p90 >= 8_850_000 && p90 <= 9_050_000, "merged p90 was " + p90);
    }

    @Test
    public void testMergeWithoutPercentileTracking()
    {
        // If neither metric has percentile tracking, merge should work regardless of bucket width
        RuntimeMetric m1 = new RuntimeMetric("latency", NANO, false, 1_000_000);
        RuntimeMetric m2 = new RuntimeMetric("latency", NANO, false, 10_000_000);

        for (int i = 0; i < 50; i++) {
            m1.addValue(i * 1_000_000);
            m2.addValue(i * 1_000_000);
        }

        // This should succeed since no histogram data to merge
        m1.mergeWith(m2);

        assertEquals(m1.getCount(), 100);
        assertFalse(m1.isPercentileTrackingEnabled());
    }

    @Test
    public void testMergeWithOnlyOneHavingPercentiles()
    {
        // If only one metric has percentile tracking, merge should work
        RuntimeMetric m1 = new RuntimeMetric("latency", NANO, true, 1_000_000);
        RuntimeMetric m2 = new RuntimeMetric("latency", NANO, false); // No percentile tracking

        for (int i = 0; i < 50; i++) {
            m1.addValue(i * 1_000_000);
            m2.addValue(i * 1_000_000);
        }

        // This should succeed - only basic stats are merged from m2
        m1.mergeWith(m2);

        assertEquals(m1.getCount(), 100);
        assertTrue(m1.isPercentileTrackingEnabled());
        // Percentiles are only from m1's data
        assertTrue(m1.getP90() != null);
    }

    @Test
    public void testSingleValueMetric()
    {
        RuntimeMetric metric = new RuntimeMetric("latency", NANO, true);
        metric.addValue(5_000_000); // 5ms

        assertEquals(metric.getCount(), 1);
        assertEquals(metric.getMin(), 5_000_000);
        assertEquals(metric.getMax(), 5_000_000);

        // All percentiles should return the same value (bucket midpoint)
        Long p90 = metric.getP90();
        Long p95 = metric.getP95();
        Long p99 = metric.getP99();

        assertTrue(p90 != null);
        assertTrue(p95 != null);
        assertTrue(p99 != null);

        // All should be close to the single value (within bucket width)
        assertTrue(Math.abs(p90 - 5_000_000) <= 1_000_000, "p90 was " + p90);
        assertTrue(Math.abs(p95 - 5_000_000) <= 1_000_000, "p95 was " + p95);
        assertTrue(Math.abs(p99 - 5_000_000) <= 1_000_000, "p99 was " + p99);
    }

    @Test
    public void testAllValuesInSameBucket()
    {
        RuntimeMetric metric = new RuntimeMetric("latency", NANO, true);

        // Add 100 values all in the same 1ms bucket (500-600μs)
        for (int i = 0; i < 100; i++) {
            metric.addValue(500_000 + i * 1000); // 500μs to 599μs
        }

        assertEquals(metric.getCount(), 100);

        // All percentiles should return the same bucket midpoint
        Long p90 = metric.getP90();
        Long p95 = metric.getP95();
        Long p99 = metric.getP99();

        // All should be the midpoint of bucket 0 (0-1ms) = 500μs
        assertEquals(p90, 500_000L);
        assertEquals(p95, 500_000L);
        assertEquals(p99, 500_000L);
    }

    @Test
    public void testNegativeValues()
    {
        RuntimeMetric metric = new RuntimeMetric("test", NANO, true);

        // Add some negative values (edge case, shouldn't happen in practice but should be handled)
        metric.addValue(-1_000_000);
        metric.addValue(-500_000);
        metric.addValue(1_000_000);
        metric.addValue(2_000_000);

        assertEquals(metric.getCount(), 4);
        assertEquals(metric.getMin(), -1_000_000);

        // Negative values go to bucket 0, so percentiles should still work
        Long p90 = metric.getP90();
        assertTrue(p90 != null);
    }

    @Test
    public void testOverflowBucketWithManyOutliers()
    {
        // Default NANO config: 1000 buckets × 1ms = 1 second max
        RuntimeMetric metric = new RuntimeMetric("latency", NANO, true);

        // Add 90 values in normal range (0-880ms)
        for (int i = 0; i < 90; i++) {
            metric.addValue(i * 10_000_000); // 0, 10ms, 20ms, ..., 890ms (last value is 89 * 10ms = 890ms)
        }

        // Add 10 outliers beyond range (5-5.009 seconds)
        for (int i = 0; i < 10; i++) {
            metric.addValue((5000000 + i * 1_000) * 1_000L); // 5000ms, 5001ms, ..., 5009ms (5-5.009 seconds)
        }

        assertEquals(metric.getCount(), 100);
        assertEquals(metric.getMax(), 5_009_000_000L); // 5.009 seconds

        // p90 should still be in normal range since 90% of values are < 1s
        Long p90 = metric.getP90();
        assertTrue(p90 != null);
        assertTrue(p90 < 1_000_000_000, "p90 was " + p90 + " (should be < 1 second)");

        // p99 will include outliers (99th value is one of the outliers)
        Long p99 = metric.getP99();
        assertTrue(p99 != null);
        // p99 will be in the overflow bucket, returning max value
        assertTrue(p99 >= 900_000_000, "p99 was " + p99);
    }

    @Test
    public void testVeryLargeValues()
    {
        RuntimeMetric metric = new RuntimeMetric("data_size", RuntimeUnit.BYTE, true);

        // Add values that will overflow the default range (1000 buckets × 1KB = 1MB)
        metric.addValue(100_000_000); // 100MB
        metric.addValue(200_000_000); // 200MB
        metric.addValue(300_000_000); // 300MB

        assertEquals(metric.getCount(), 3);
        assertEquals(metric.getMax(), 300_000_000);

        // All values overflow, so all go to bucket 999
        Long p90 = metric.getP90();
        assertTrue(p90 != null);
        // Should return the overflow bucket midpoint or max
        assertTrue(p90 > 0);
    }

    @Test
    public void testZeroValues()
    {
        RuntimeMetric metric = new RuntimeMetric("latency", NANO, true);

        // Add all zeros
        for (int i = 0; i < 100; i++) {
            metric.addValue(0);
        }

        assertEquals(metric.getCount(), 100);
        assertEquals(metric.getMin(), 0);
        assertEquals(metric.getMax(), 0);

        // All percentiles should be 0 (midpoint of bucket 0)
        Long p90 = metric.getP90();
        Long p95 = metric.getP95();
        Long p99 = metric.getP99();

        assertTrue(p90 != null);
        assertTrue(p95 != null);
        assertTrue(p99 != null);

        // Should all return bucket 0 midpoint (500μs for 1ms bucket width)
        assertTrue(p90 >= 0 && p90 <= 1_000_000, "p90 was " + p90);
        assertTrue(p95 >= 0 && p95 <= 1_000_000, "p95 was " + p95);
        assertTrue(p99 >= 0 && p99 <= 1_000_000, "p99 was " + p99);
    }

    @Test
    public void testSmallCountPercentiles()
    {
        RuntimeMetric metric = new RuntimeMetric("latency", NANO, true);

        // Add only 2 values
        metric.addValue(1_000_000); // 1ms
        metric.addValue(9_000_000); // 9ms

        assertEquals(metric.getCount(), 2);

        // Percentiles should still work but may not be meaningful
        Long p90 = metric.getP90();
        Long p95 = metric.getP95();
        Long p99 = metric.getP99();

        assertTrue(p90 != null);
        assertTrue(p95 != null);
        assertTrue(p99 != null);

        // All percentiles point to bucket 9 (9ms value)
        // Bucket 9 midpoint = 9.5ms, but clamped to max = 9ms
        assertEquals(p90, Long.valueOf(9_000_000), "p90 should be clamped to max (9ms)");
        assertEquals(p95, Long.valueOf(9_000_000), "p95 should be clamped to max (9ms)");
        assertEquals(p99, Long.valueOf(9_000_000), "p99 should be clamped to max (9ms)");
    }

    @Test
    public void testCachedPercentilesAfterCopy()
    {
        RuntimeMetric original = new RuntimeMetric("latency", NANO, true);

        // Add values and compute percentiles
        for (int i = 0; i < 100; i++) {
            original.addValue(i * 1_000_000);
        }

        Long originalP90 = original.getP90();

        // Copy the metric
        RuntimeMetric copy = RuntimeMetric.copyOf(original);

        // Copy should have the same cached percentiles
        assertEquals(copy.getP90(), originalP90);

        // Add more data to original
        for (int i = 100; i < 200; i++) {
            original.addValue(i * 1_000_000);
        }

        // Original's cached percentile is now stale (but that's acceptable)
        // It will still return the old cached value until invalidated
        // Copy's percentiles should remain unchanged
        assertEquals(copy.getP90(), originalP90);
    }

    @Test
    public void testSkewedDistribution()
    {
        RuntimeMetric metric = new RuntimeMetric("latency", NANO, true);

        // Add 95 fast values (0-94ms)
        for (int i = 0; i < 95; i++) {
            metric.addValue(i * 1_000_000);
        }

        // Add 5 very slow values (500-504ms)
        for (int i = 500; i < 505; i++) {
            metric.addValue(i * 1_000_000);
        }

        assertEquals(metric.getCount(), 100);

        // p90 should still be in the fast range
        Long p90 = metric.getP90();
        assertTrue(p90 != null);
        assertTrue(p90 >= 88_000_000 && p90 <= 92_000_000, "p90 was " + p90 + " (should be ~90ms)");

        // p95 and p99 should reflect the slow outliers
        Long p95 = metric.getP95();
        Long p99 = metric.getP99();
        assertTrue(p95 != null);
        assertTrue(p99 != null);
        assertTrue(p95 > 94_000_000, "p95 was " + p95 + " (should include slow values)");
        assertTrue(p99 > 94_000_000, "p99 was " + p99 + " (should include slow values)");
    }
}
