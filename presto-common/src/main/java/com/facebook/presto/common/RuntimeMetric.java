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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.google.errorprone.annotations.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.common.RuntimeUnit.NONE;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;
import static java.util.Objects.requireNonNull;

/**
 * A metric exposed by a presto operator or connector. It will be aggregated at the query level.
 */
@ThriftStruct
public class RuntimeMetric
{
    private static final List<RuntimeMetricEvent> EMPTY_EVENTS = Collections.emptyList();

    private final String name;
    private final RuntimeUnit unit;
    private final AtomicLong sum = new AtomicLong();
    private final AtomicLong count = new AtomicLong();
    private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);
    private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
    private volatile List<RuntimeMetricEvent> events = EMPTY_EVENTS;

    /**
     * Creates a new empty RuntimeMetric.
     *
     * @param name Name of this metric. If used in the presto core code base, this should be a value defined in {@link RuntimeMetricName}. But connectors could use arbitrary names.
     * @param unit Unit of this metric. Available units are defined in {@link RuntimeUnit}.
     */
    public RuntimeMetric(String name, RuntimeUnit unit)
    {
        this.name = requireNonNull(name, "name is null");
        this.unit = unit == null ? NONE : unit;
    }

    public static RuntimeMetric copyOf(RuntimeMetric metric)
    {
        requireNonNull(metric, "metric is null");
        return new RuntimeMetric(metric.getName(), metric.getUnit(), metric.getSum(), metric.getCount(), metric.getMax(), metric.getMin(), metric.getEventsSnapshot());
    }

    @JsonCreator
    @ThriftConstructor
    public RuntimeMetric(
            @JsonProperty("name") String name,
            @JsonProperty("unit") RuntimeUnit unit,
            @JsonProperty("sum") long sum,
            @JsonProperty("count") long count,
            @JsonProperty("max") long max,
            @JsonProperty("min") long min)
    {
        this(name, unit, sum, count, max, min, EMPTY_EVENTS);
    }

    private RuntimeMetric(
            String name,
            RuntimeUnit unit,
            long sum,
            long count,
            long max,
            long min,
            List<RuntimeMetricEvent> events)
    {
        this(name, unit);
        set(sum, count, max, min);
        addEvents(events);
    }

    private void set(long sum, long count, long max, long min)
    {
        this.sum.set(sum);
        this.count.set(count);
        // Keep the accumulation sentinels for empty metrics so a later addValue stays correct.
        this.max.set(count == 0 ? Long.MIN_VALUE : max);
        this.min.set(count == 0 ? Long.MAX_VALUE : min);
    }

    /**
     * Overwrites the aggregate values with {@code metric}'s values, leaving trace events untouched.
     */
    public void set(RuntimeMetric metric)
    {
        requireNonNull(metric, "metric is null");
        checkState(unit == metric.getUnit(), "The metric must have the same unit type as the current one.");
        set(metric.getSum(), metric.getCount(), metric.getMax(), metric.getMin());
    }

    @JsonProperty
    @ThriftField(1)
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
        checkState(metric1.getUnit() == metric2.getUnit(), "Two metrics to be merged must have the same unit type.");

        RuntimeMetric mergedMetric = copyOf(metric1);
        mergedMetric.mergeWith(metric2);
        return mergedMetric;
    }

    /**
     * Merges {@code metric}'s aggregates and trace events into this object.
     */
    public void mergeWith(RuntimeMetric metric)
    {
        if (metric == null) {
            return;
        }
        checkState(unit == metric.getUnit(), "The metric to be merged must have the same unit type as the current one.");
        sum.addAndGet(metric.getSum());
        count.addAndGet(metric.getCount());
        // An empty metric carries no min/max, so skip it to avoid pulling the sentinel toward zero.
        if (metric.getCount() > 0) {
            max.accumulateAndGet(metric.getMax(), Math::max);
            min.accumulateAndGet(metric.getMin(), Math::min);
        }
        addEvents(metric.getEventsSnapshot());
    }

    @JsonProperty
    @ThriftField(2)
    public long getSum()
    {
        return sum.get();
    }

    @JsonProperty
    @ThriftField(3)
    public long getCount()
    {
        return count.get();
    }

    @JsonProperty
    @ThriftField(4)
    public long getMax()
    {
        long value = max.get();
        return value == Long.MIN_VALUE ? 0 : value;
    }

    @JsonProperty
    @ThriftField(5)
    public long getMin()
    {
        long value = min.get();
        return value == Long.MAX_VALUE ? 0 : value;
    }

    @JsonProperty
    @ThriftField(6)
    public RuntimeUnit getUnit()
    {
        return unit;
    }

    synchronized void addEvent(RuntimeMetricEvent event)
    {
        getOrCreateEvents().add(requireNonNull(event, "event is null"));
    }

    /**
     * Returns coordinator-local trace events. Trace events are JSON-only and are not
     * included in Thrift serialization.
     */
    @JsonProperty
    @JsonInclude(NON_EMPTY)
    public List<RuntimeMetricEvent> getEvents()
    {
        List<RuntimeMetricEvent> result = getEventsSnapshot();
        if (result.isEmpty()) {
            return EMPTY_EVENTS;
        }
        return Collections.unmodifiableList(result);
    }

    @JsonSetter("events")
    public void setEvents(List<RuntimeMetricEvent> events)
    {
        replaceEvents(events == null ? EMPTY_EVENTS : events);
    }

    private List<RuntimeMetricEvent> getEventsSnapshot()
    {
        if (events == EMPTY_EVENTS) {
            return EMPTY_EVENTS;
        }
        synchronized (this) {
            if (events == EMPTY_EVENTS) {
                return EMPTY_EVENTS;
            }
            return new ArrayList<>(events);
        }
    }

    private void replaceEvents(List<RuntimeMetricEvent> newEvents)
    {
        if (events == EMPTY_EVENTS && newEvents.isEmpty()) {
            return;
        }
        synchronized (this) {
            events = EMPTY_EVENTS;
            if (!newEvents.isEmpty()) {
                getOrCreateEvents().addAll(newEvents);
            }
        }
    }

    private void addEvents(List<RuntimeMetricEvent> newEvents)
    {
        if (newEvents.isEmpty()) {
            return;
        }
        synchronized (this) {
            getOrCreateEvents().addAll(newEvents);
        }
    }

    @GuardedBy("this")
    private List<RuntimeMetricEvent> getOrCreateEvents()
    {
        if (events == EMPTY_EVENTS) {
            events = new ArrayList<>();
        }
        return events;
    }

    private static void checkState(boolean condition, String message)
    {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
