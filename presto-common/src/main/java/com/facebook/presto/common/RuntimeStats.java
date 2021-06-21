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

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Metrics exposed by presto operators or connectors. These will be aggregated at the query level.
 */
public class RuntimeStats
{
    private final Map<String, RuntimeMetric> metrics;

    public RuntimeStats()
    {
        metrics = new HashMap<>();
    }

    @JsonCreator
    public RuntimeStats(Map<String, RuntimeMetric> metrics)
    {
        this.metrics = requireNonNull(metrics, "metrics is null");
    }

    public static RuntimeStats copyOf(RuntimeStats stats)
    {
        requireNonNull(stats, "stats is null");
        RuntimeStats statsCopy = new RuntimeStats();
        statsCopy.mergeWith(stats);
        return statsCopy;
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
        return metrics;
    }

    public void addMetricValue(String name, long value)
    {
        metrics.computeIfAbsent(name, RuntimeMetric::new);
        metrics.get(name).addValue(value);
    }

    /**
     * Merges {@code stats} into this object.
     */
    public void mergeWith(RuntimeStats stats)
    {
        if (stats == null) {
            return;
        }
        stats.getMetrics().values().forEach(metric -> {
            metrics.computeIfAbsent(metric.getName(), RuntimeMetric::new);
            metrics.get(metric.getName()).mergeWith(metric);
        });
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
        stats.getMetrics().values().forEach(metric -> {
            metrics.put(metric.getName(), RuntimeMetric.copyOf(metric));
        });
    }
}
