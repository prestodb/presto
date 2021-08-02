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

import static java.util.Objects.requireNonNull;

/**
 * A metric exposed by a presto operator or connector. It will be aggregated at the query level.
 */
public class RuntimeMetric
{
    private final String name;
    private long sum;
    private long count;
    private long max = Long.MIN_VALUE;
    private long min = Long.MAX_VALUE;

    /**
     * Creates a new empty RuntimeMetric.
     *
     * @param name Name of this metric. If used in the presto core code base, this should be a value defined in {@link RuntimeMetricName}. But connectors could use arbitrary names.
     */
    public RuntimeMetric(String name)
    {
        this.name = name;
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
        this.name = requireNonNull(name, "name is null");
        this.sum = sum;
        this.count = count;
        this.max = max;
        this.min = min;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    public void addValue(long value)
    {
        sum += value;
        count++;
        max = Math.max(max, value);
        min = Math.min(min, value);
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
        sum += metric.getSum();
        count += metric.getCount();
        max = Math.max(max, metric.getMax());
        min = Math.min(min, metric.getMin());
    }

    @JsonProperty
    public long getSum()
    {
        return sum;
    }

    @JsonProperty
    public long getCount()
    {
        return count;
    }

    @JsonProperty
    public long getMax()
    {
        return max;
    }

    @JsonProperty
    public long getMin()
    {
        return min;
    }
}
