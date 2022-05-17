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
import static org.testng.Assert.assertNull;

public class TestRuntimeStats
{
    private static final String TEST_METRIC_NAME_1 = "test1";
    private static final String TEST_METRIC_NAME_2 = "test2";
    private static final String TEST_METRIC_NAME_3 = "test3";
    private static final String TEST_METRIC_NAME_NANO_1 = "test_nano_1";
    private static final String TEST_METRIC_NAME_NANO_2 = "test_nano_2";
    private static final String TEST_METRIC_NAME_BYTE = "test_byte";

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
    public void testAddMetricValue()
    {
        RuntimeStats stats = new RuntimeStats();
        stats.addMetricValue(TEST_METRIC_NAME_1, NONE, 2);
        stats.addMetricValue(TEST_METRIC_NAME_1, NONE, 3);
        stats.addMetricValue(TEST_METRIC_NAME_1, NONE, 5);
        stats.addMetricValue(TEST_METRIC_NAME_NANO_1, NANO, 7);

        assertRuntimeMetricEquals(
                stats.getMetric(TEST_METRIC_NAME_1),
                new RuntimeMetric(TEST_METRIC_NAME_1, NONE, 10, 3, 5, 2));
        assertRuntimeMetricEquals(
                stats.getMetric(TEST_METRIC_NAME_NANO_1),
                new RuntimeMetric(TEST_METRIC_NAME_NANO_1, NANO, 7, 1, 7, 7));

        stats.reset();
        assertEquals(stats.getMetrics().size(), 0);
    }

    @Test
    public void testMergeMetric()
    {
        RuntimeStats stats1 = new RuntimeStats();
        stats1.addMetricValue(TEST_METRIC_NAME_1, NONE, 2);
        stats1.addMetricValue(TEST_METRIC_NAME_1, NONE, 3);
        stats1.addMetricValue(TEST_METRIC_NAME_NANO_1, NANO, 3);

        RuntimeStats stats2 = new RuntimeStats();
        stats2.addMetricValue(TEST_METRIC_NAME_NANO_2, NANO, 5);
        stats2.mergeMetric(TEST_METRIC_NAME_2, stats1.getMetric(TEST_METRIC_NAME_1));
        stats2.mergeMetric(TEST_METRIC_NAME_NANO_2, stats1.getMetric(TEST_METRIC_NAME_NANO_1));

        assertEquals(stats2.getMetrics().size(), 2);
        assertRuntimeMetricEquals(
                stats2.getMetric(TEST_METRIC_NAME_2),
                new RuntimeMetric(TEST_METRIC_NAME_2, NONE, 5, 2, 3, 2));
        assertRuntimeMetricEquals(
                stats2.getMetric(TEST_METRIC_NAME_NANO_2),
                new RuntimeMetric(TEST_METRIC_NAME_NANO_2, NANO, 8, 2, 5, 3));
    }

    @Test(expectedExceptions = {IllegalStateException.class})
    public void testMergeMetricWithConflictUnits()
    {
        RuntimeStats stats1 = new RuntimeStats();
        stats1.addMetricValue(TEST_METRIC_NAME_NANO_1, NANO, 3);

        RuntimeStats stats2 = new RuntimeStats();
        stats2.addMetricValue(TEST_METRIC_NAME_BYTE, BYTE, 3);
        stats2.mergeMetric(TEST_METRIC_NAME_BYTE, stats1.getMetric(TEST_METRIC_NAME_NANO_1));
    }

    @Test
    public void testMerge()
    {
        RuntimeStats stats1 = new RuntimeStats();
        stats1.addMetricValue(TEST_METRIC_NAME_1, NONE, 2);
        stats1.addMetricValue(TEST_METRIC_NAME_1, NONE, 3);
        stats1.addMetricValue(TEST_METRIC_NAME_2, NONE, 1);
        stats1.addMetricValue(TEST_METRIC_NAME_2, NONE, 2);
        stats1.addMetricValue(TEST_METRIC_NAME_NANO_1, NANO, 2);
        stats1.addMetricValue(TEST_METRIC_NAME_BYTE, BYTE, 1);

        RuntimeStats stats2 = new RuntimeStats();
        stats2.addMetricValue(TEST_METRIC_NAME_2, NONE, 0);
        stats2.addMetricValue(TEST_METRIC_NAME_2, NONE, 3);
        stats2.addMetricValue(TEST_METRIC_NAME_3, NONE, 8);
        stats2.addMetricValue(TEST_METRIC_NAME_BYTE, BYTE, 3);

        RuntimeStats mergedStats = RuntimeStats.merge(stats1, stats2);
        assertRuntimeMetricEquals(
                mergedStats.getMetric(TEST_METRIC_NAME_1),
                new RuntimeMetric(TEST_METRIC_NAME_1, NONE, 5, 2, 3, 2));
        assertRuntimeMetricEquals(
                mergedStats.getMetric(TEST_METRIC_NAME_2),
                new RuntimeMetric(TEST_METRIC_NAME_2, NONE, 6, 4, 3, 0));
        assertRuntimeMetricEquals(
                mergedStats.getMetric(TEST_METRIC_NAME_3),
                new RuntimeMetric(TEST_METRIC_NAME_3, NONE, 8, 1, 8, 8));
        assertRuntimeMetricEquals(
                mergedStats.getMetric(TEST_METRIC_NAME_NANO_1),
                new RuntimeMetric(TEST_METRIC_NAME_NANO_1, NANO, 2, 1, 2, 2));
        assertRuntimeMetricEquals(
                mergedStats.getMetric(TEST_METRIC_NAME_BYTE),
                new RuntimeMetric(TEST_METRIC_NAME_BYTE, BYTE, 4, 2, 3, 1));

        stats1.mergeWith(stats2);
        mergedStats.getMetrics().values().forEach(metric -> assertRuntimeMetricEquals(metric, stats1.getMetric(metric.getName())));
        assertEquals(mergedStats.getMetrics().size(), stats1.getMetrics().size());
    }

    @Test(expectedExceptions = {IllegalStateException.class})
    public void testMergeWithConflictUnits()
    {
        RuntimeStats stats1 = new RuntimeStats();
        stats1.addMetricValue(TEST_METRIC_NAME_BYTE, NANO, 1);

        RuntimeStats stats2 = new RuntimeStats();
        stats2.addMetricValue(TEST_METRIC_NAME_BYTE, BYTE, 3);

        RuntimeStats.merge(stats1, stats2);
    }

    @Test
    public void testMergeWithNull()
    {
        RuntimeStats stats = new RuntimeStats();
        stats.addMetricValue(TEST_METRIC_NAME_1, NONE, 2);
        stats.mergeWith(null);
        assertRuntimeMetricEquals(
                stats.getMetric(TEST_METRIC_NAME_1),
                new RuntimeMetric(TEST_METRIC_NAME_1, NONE, 2, 1, 2, 2));
    }

    @Test
    public void testUpdate()
    {
        RuntimeStats stats1 = new RuntimeStats();
        stats1.addMetricValue(TEST_METRIC_NAME_1, NONE, 2);
        stats1.update(null);
        assertRuntimeMetricEquals(
                stats1.getMetric(TEST_METRIC_NAME_1),
                new RuntimeMetric(TEST_METRIC_NAME_1, NONE, 2, 1, 2, 2));

        RuntimeStats stats2 = new RuntimeStats();
        stats2.addMetricValue(TEST_METRIC_NAME_2, NONE, 2);
        stats1.update(stats2);
        assertRuntimeMetricEquals(
                stats1.getMetric(TEST_METRIC_NAME_1),
                new RuntimeMetric(TEST_METRIC_NAME_1, NONE, 2, 1, 2, 2));
        assertRuntimeMetricEquals(
                stats1.getMetric(TEST_METRIC_NAME_2),
                stats1.getMetric(TEST_METRIC_NAME_2));

        stats2.addMetricValue(TEST_METRIC_NAME_2, NONE, 4);
        stats1.update(stats2);
        assertRuntimeMetricEquals(
                stats1.getMetric(TEST_METRIC_NAME_2),
                stats1.getMetric(TEST_METRIC_NAME_2));

        stats2.addMetricValue(TEST_METRIC_NAME_NANO_1, NANO, 4);
        stats1.update(stats2);
        assertRuntimeMetricEquals(
                stats1.getMetric(TEST_METRIC_NAME_NANO_1),
                stats1.getMetric(TEST_METRIC_NAME_NANO_1));
    }

    @Test(expectedExceptions = {IllegalStateException.class})
    public void testUpdateWithConflictUnits()
    {
        RuntimeStats stats1 = new RuntimeStats();
        stats1.addMetricValue(TEST_METRIC_NAME_BYTE, BYTE, 4);

        RuntimeStats stats2 = new RuntimeStats();
        stats2.addMetricValue(TEST_METRIC_NAME_BYTE, NANO, 4);

        stats1.update(stats2);
    }

    @Test
    public void testJson()
    {
        RuntimeStats stats = new RuntimeStats();
        stats.addMetricValue(TEST_METRIC_NAME_1, NONE, 2);
        stats.addMetricValue(TEST_METRIC_NAME_1, NONE, 3);
        stats.addMetricValue(TEST_METRIC_NAME_2, NONE, 8);
        stats.addMetricValue(TEST_METRIC_NAME_3, NONE, 8);
        stats.addMetricValue(TEST_METRIC_NAME_NANO_1, NANO, 8);
        stats.addMetricValue(TEST_METRIC_NAME_BYTE, BYTE, 8);

        JsonCodec<RuntimeStats> codec = JsonCodec.jsonCodec(RuntimeStats.class);
        String json = codec.toJson(stats);
        RuntimeStats actual = codec.fromJson(json);

        actual.getMetrics().forEach((name, metric) -> assertRuntimeMetricEquals(metric, stats.getMetric(name)));
    }

    @Test
    public void testNullJson()
    {
        JsonCodec<RuntimeStats> codec = JsonCodec.jsonCodec(RuntimeStats.class);
        String nullJson = codec.toJson(null);
        RuntimeStats actual = codec.fromJson(nullJson);
        assertNull(actual);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testReturnUnmodifiedMetrics()
    {
        RuntimeStats stats = new RuntimeStats();
        stats.getMetrics().put(TEST_METRIC_NAME_1, new RuntimeMetric(TEST_METRIC_NAME_1, NONE));
    }
}
