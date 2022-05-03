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

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Metrics exposed by presto operators or connectors. These will be aggregated at the query level.
 */
@ThriftStruct
public class RuntimeStats
{
    private final ConcurrentMap<RuntimeMetricKey, RuntimeMetric> metricMap = new ConcurrentHashMap<>();

    public RuntimeStats()
    {
    }

    @JsonCreator
    @ThriftConstructor
    public RuntimeStats(Collection<RuntimeMetric> metrics)
    {
        requireNonNull(metrics, "metrics is null");
        metrics.forEach((metric) -> this.metricMap.computeIfAbsent(metric.getMetricKey(), RuntimeMetric::new).mergeWith(metric));
    }

    public static RuntimeStats copyOf(RuntimeStats stats)
    {
        return new RuntimeStats(stats.getMetrics());
    }

    /**
     * Merges {@code stats1} and {@code stats2} and returns the result. The input parameters are not updated.
     */
    public static RuntimeStats merge(RuntimeStats stats1, RuntimeStats stats2)
    {
        if (stats1 == null) {
            return stats2;
        }
        if (stats2 == null) {
            return stats1;
        }
        RuntimeStats mergedStats = copyOf(stats1);
        mergedStats.mergeWith(stats2);
        return mergedStats;
    }

    public void reset()
    {
        metricMap.clear();
    }

    public RuntimeMetric getMetric(RuntimeMetricKey key)
    {
        return metricMap.get(key);
    }

    public Map<RuntimeMetricKey, RuntimeMetric> getMetricMap()
    {
        return Collections.unmodifiableMap(metricMap);
    }

    @JsonValue
    @ThriftField(1)
    public Collection<RuntimeMetric> getMetrics()
    {
        return Collections.unmodifiableCollection(metricMap.values());
    }

    public void addMetricValue(String name, long value)
    {
        addMetricValue(new RuntimeMetricKey(name), value);
    }

    public void addMetricValue(RuntimeMetricKey key, long value)
    {
        metricMap.computeIfAbsent(key, RuntimeMetric::new).addValue(value);
    }

    public void addMetricValueIgnoreZero(RuntimeMetricKey key, long value)
    {
        if (value == 0) {
            return;
        }
        addMetricValue(key, value);
    }

    /**
     * Merges {@code metric} into this object with key {@code key}.
     */
    public void mergeMetric(RuntimeMetricKey key, RuntimeMetric metric)
    {
        metricMap.computeIfAbsent(key, RuntimeMetric::new).mergeWith(metric);
    }

    /**
     * Merges {@code stats} into this object.
     */
    public void mergeWith(RuntimeStats stats)
    {
        if (stats == null) {
            return;
        }
        stats.getMetricMap().forEach((key, newMetric) -> metricMap.computeIfAbsent(key, RuntimeMetric::new).mergeWith(newMetric));
    }

    /**
     * Updates the metrics according to their values in {@code stats}.
     * Metrics not included in {@code stats} will not be changed.
     */
    public void update(RuntimeStats stats)
    {
        if (stats == null) {
            return;
        }
        stats.getMetricMap().forEach((key, newMetric) -> metricMap.computeIfAbsent(key, RuntimeMetric::new).set(newMetric));
    }

    public <V> V profileNanos(RuntimeMetricKey key, Supplier<V> supplier)
    {
        long startTime = System.nanoTime();
        V result = supplier.get();
        addMetricValueIgnoreZero(key, System.nanoTime() - startTime);
        return result;
    }
}
