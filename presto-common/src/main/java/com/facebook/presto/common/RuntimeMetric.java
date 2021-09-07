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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

/**
 * A metric exposed by a presto operator or connector. It will be aggregated at the query level.
 */
public class RuntimeMetric
{
    private final String name;
    private final AtomicLong sum = new AtomicLong();
    private final AtomicLong count = new AtomicLong();
    private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);
    private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);

    /**
     * Creates a new empty RuntimeMetric.
     *
     * @param name Name of this metric. If used in the presto core code base, this should be a value defined in {@link RuntimeMetricName}. But connectors could use arbitrary names.
     */
    public RuntimeMetric(String name)
    {
        this.name = requireNonNull(name, "name is null");
    }

    public static RuntimeMetric copyOf(RuntimeMetric metric)
    {
        requireNonNull(metric, "metric is null");
        return new RuntimeMetric(metric.getName(), metric.getSum(), metric.getCount(), metric.getMax(), metric.getMin());
    }

    @JsonCreator
    public RuntimeMetric(
            @JsonProperty("name") String name,
            @JsonProperty("sum") long sum,
            @JsonProperty("count") long count,
            @JsonProperty("max") long max,
            @JsonProperty("min") long min)
    {
        this(name);
        set(sum, count, max, min);
    }

    private void set(long sum, long count, long max, long min)
    {
        this.sum.set(sum);
        this.count.set(count);
        this.max.set(max);
        this.min.set(min);
    }

    public void set(RuntimeMetric metric)
    {
        requireNonNull(metric, "metric is null");
        set(metric.getSum(), metric.getCount(), metric.getMax(), metric.getMin());
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    public void addValue(long value)
    {
        sum.addAndGet(value);
        count.incrementAndGet();
        max.accumulateAndGet(value, Math::max);
        min.accumulateAndGet(value, Math::min);
    }

    /**
     * Merges {@code metric1} and {@code metric2} and returns the result. The input parameters are not updated.
     */
    public static RuntimeMetric merge(RuntimeMetric metric1, RuntimeMetric metric2)
    {
        if (metric1 == null) {
            return metric2;
        }
        if (metric2 == null) {
            return metric1;
        }
        RuntimeMetric mergedMetric = copyOf(metric1);
        mergedMetric.mergeWith(metric2);
        return mergedMetric;
    }

    /**
     * Merges {@code metric} into this object.
     */
    public void mergeWith(RuntimeMetric metric)
    {
        if (metric == null) {
            return;
        }
        sum.addAndGet(metric.getSum());
        count.addAndGet(metric.getCount());
        max.accumulateAndGet(metric.getMax(), Math::max);
        min.accumulateAndGet(metric.getMin(), Math::min);
    }

    @JsonProperty
    public long getSum()
    {
        return sum.get();
    }

    @JsonProperty
    public long getCount()
    {
        return count.get();
    }

    @JsonProperty
    public long getMax()
    {
        return max.get();
    }

    @JsonProperty
    public long getMin()
    {
        return min.get();
    }
}
