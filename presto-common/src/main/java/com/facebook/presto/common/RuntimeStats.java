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
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Metrics exposed by presto operators or connectors. These will be aggregated at the query level.
 */
public class RuntimeStats
{
    private final ConcurrentMap<String, RuntimeMetric> metrics = new ConcurrentHashMap<>();

    public RuntimeStats()
    {
    }

    @JsonCreator
    public RuntimeStats(Map<String, RuntimeMetric> metrics)
    {
        requireNonNull(metrics, "metrics is null");
        metrics.forEach((name, newMetric) -> this.metrics.computeIfAbsent(name, RuntimeMetric::new).mergeWith(newMetric));
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
        metrics.clear();
    }

    public RuntimeMetric getMetric(String name)
    {
        return metrics.get(name);
    }

    @JsonValue
    public Map<String, RuntimeMetric> getMetrics()
    {
        return Collections.unmodifiableMap(metrics);
    }

    public void addMetricValue(String name, long value)
    {
        metrics.computeIfAbsent(name, RuntimeMetric::new).addValue(value);
    }

    /**
     * Merges {@code metric} into this object with name {@code name}.
     */
    public void mergeMetric(String name, RuntimeMetric metric)
    {
        metrics.computeIfAbsent(name, RuntimeMetric::new).mergeWith(metric);
    }

    /**
     * Merges {@code stats} into this object.
     */
    public void mergeWith(RuntimeStats stats)
    {
        if (stats == null) {
            return;
        }
        stats.getMetrics().forEach((name, newMetric) -> metrics.computeIfAbsent(name, RuntimeMetric::new).mergeWith(newMetric));
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
        stats.getMetrics().forEach((name, newMetric) -> metrics.computeIfAbsent(name, RuntimeMetric::new).set(newMetric));
    }

    public <V> V profileNanos(String tag, Supplier<V> supplier)
    {
        long startTime = System.nanoTime();
        V result = supplier.get();
        addMetricValue(tag, System.nanoTime() - startTime);
        return result;
    }
}
