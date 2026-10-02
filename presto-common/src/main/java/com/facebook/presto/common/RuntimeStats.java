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
import jakarta.annotation.Nullable;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import static com.facebook.presto.common.RuntimeUnit.NANO;
import static java.util.Objects.requireNonNull;

/**
 * Metrics exposed by presto operators or connectors. These will be aggregated at the query level.
 */
@ThriftStruct
public class RuntimeStats
{
    public static final int DEFAULT_QUERY_TRACE_MAX_EVENTS = 2_000;

    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

    private final ConcurrentMap<String, RuntimeMetric> metrics = new ConcurrentHashMap<>();
    @Nullable
    private volatile QueryTracer queryTracer;

    public RuntimeStats()
    {
    }

    @JsonCreator
    @ThriftConstructor
    public RuntimeStats(Map<String, RuntimeMetric> metrics)
    {
        requireNonNull(metrics, "metrics is null");
        metrics.forEach(this::mergeMetric);
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

    public synchronized void reset()
    {
        QueryTracer queryTracer = this.queryTracer;
        if (queryTracer != null) {
            queryTracer.reset();
        }
        metrics.clear();
    }

    public RuntimeMetric getMetric(String name)
    {
        return metrics.get(name);
    }

    @JsonValue
    @ThriftField(1)
    public Map<String, RuntimeMetric> getMetrics()
    {
        return Collections.unmodifiableMap(metrics);
    }

    public void addMetricValue(String name, RuntimeUnit unit, long value)
    {
        metrics.computeIfAbsent(name, k -> new RuntimeMetric(name, unit)).addValue(value);
    }

    public void addMetricValueIgnoreZero(String name, RuntimeUnit unit, long value)
    {
        if (value == 0) {
            return;
        }
        addMetricValue(name, unit, value);
    }

    /**
     * Merges {@code metric} into this object with name {@code name}.
     */
    public void mergeMetric(String name, RuntimeMetric metric)
    {
        metrics.computeIfAbsent(name, k -> new RuntimeMetric(name, metric.getUnit())).mergeWith(metric);
    }

    /**
     * Merges {@code stats} into this object.
     */
    public void mergeWith(RuntimeStats stats)
    {
        if (stats == null) {
            return;
        }
        stats.getMetrics().forEach(this::mergeMetric);
    }

    /**
     * Updates the metrics according to their values in {@code stats}.
     * Metrics not included in {@code stats} will not be changed.
     * Only aggregate values are overwritten; trace events already recorded locally are preserved.
     */
    public void update(RuntimeStats stats)
    {
        if (stats == null) {
            return;
        }
        if (stats == this) {
            return;
        }
        stats.getMetrics().forEach((name, newMetric) -> metrics.computeIfAbsent(name, k -> new RuntimeMetric(name, newMetric.getUnit())).set(newMetric));
    }

    /**
     * Creates the query tracer on first use. The query trace starts separately so callers can
     * establish its exact lifecycle boundary.
     */
    public synchronized QueryTracer enableTracing()
    {
        return enableTracing(DEFAULT_QUERY_TRACE_MAX_EVENTS);
    }

    /**
     * Creates the query tracer on first use with the specified event limit.
     */
    public synchronized QueryTracer enableTracing(int maxEvents)
    {
        QueryTracer queryTracer = this.queryTracer;
        if (queryTracer == null) {
            queryTracer = new QueryTracer(metrics, maxEvents);
            this.queryTracer = queryTracer;
        }
        return queryTracer;
    }

    /**
     * Returns whether this instance has been configured to collect query trace events.
     */
    public boolean isTracingEnabled()
    {
        return queryTracer != null;
    }

    /**
     * Returns the active query tracer, or {@code null} when no query trace is active.
     */
    @Nullable
    public QueryTracer getQueryTracer()
    {
        QueryTracer queryTracer = this.queryTracer;
        return queryTracer != null && queryTracer.isTracingActive() ? queryTracer : null;
    }

    /**
     * Enables tracing if necessary and starts the query trace.
     */
    public QueryTracer startQueryTrace()
    {
        return startQueryTrace(DEFAULT_QUERY_TRACE_MAX_EVENTS);
    }

    /**
     * Enables tracing if necessary and starts the query trace with the specified event limit.
     */
    public QueryTracer startQueryTrace(int maxEvents)
    {
        QueryTracer queryTracer = enableTracing(maxEvents);
        queryTracer.startQueryTrace();
        return queryTracer;
    }

    public <V> V recordWallTime(String tag, Supplier<V> supplier)
    {
        long startTime = System.nanoTime();
        QueryTracer queryTracer = this.queryTracer;
        if (queryTracer == null || !queryTracer.isTracingActive()) {
            V result = supplier.get();
            addMetricValueIgnoreZero(tag, NANO, System.nanoTime() - startTime);
            return result;
        }

        QueryTracer.TraceSpan traceSpan = queryTracer.startScopedTrace(tag, startTime);
        boolean failed = true;
        try {
            V result = supplier.get();
            failed = false;
            return result;
        }
        finally {
            long endTime = System.nanoTime();
            try {
                if (!failed) {
                    addMetricValueIgnoreZero(tag, NANO, endTime - startTime);
                }
            }
            finally {
                traceSpan.finish(endTime, failed);
            }
        }
    }

    public void recordWallTime(String tag, Runnable runnable)
    {
        recordWallTime(tag, () -> {
            runnable.run();
            return null;
        });
    }

    public <V> V recordWallAndCpuTime(String tag, Supplier<V> supplier)
    {
        long startWall = System.nanoTime();
        long startCpu = THREAD_MX_BEAN.getCurrentThreadCpuTime();
        QueryTracer queryTracer = this.queryTracer;
        if (queryTracer == null || !queryTracer.isTracingActive()) {
            V result = supplier.get();
            long endWall = System.nanoTime();
            long endCpu = THREAD_MX_BEAN.getCurrentThreadCpuTime();
            addMetricValueIgnoreZero(tag, NANO, endWall - startWall);
            addMetricValueIgnoreZero(tag + "OnCpu", NANO, endCpu - startCpu);
            return result;
        }

        // CPU time is an aggregate rather than a wall-clock interval, so only the wall metric is traced.
        QueryTracer.TraceSpan traceSpan = queryTracer.startScopedTrace(tag, startWall);
        boolean failed = true;
        try {
            V result = supplier.get();
            failed = false;
            return result;
        }
        finally {
            long endWall = System.nanoTime();
            long endCpu = THREAD_MX_BEAN.getCurrentThreadCpuTime();
            try {
                if (!failed) {
                    addMetricValueIgnoreZero(tag, NANO, endWall - startWall);
                    addMetricValueIgnoreZero(tag + "OnCpu", NANO, endCpu - startCpu);
                }
            }
            finally {
                traceSpan.finish(endWall, failed);
            }
        }
    }

    public void recordWallAndCpuTime(String tag, Runnable runnable)
    {
        recordWallAndCpuTime(tag, () -> {
            runnable.run();
            return null;
        });
    }
}
