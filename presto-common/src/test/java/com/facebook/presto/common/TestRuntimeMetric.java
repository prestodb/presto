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
}
