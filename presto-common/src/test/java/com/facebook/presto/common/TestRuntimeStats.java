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

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.common.RuntimeMetricName.QUERY_TRACE_TIME_NANOS;
import static com.facebook.presto.common.RuntimeUnit.BYTE;
import static com.facebook.presto.common.RuntimeUnit.NANO;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestRuntimeStats
{
    private static final String TEST_METRIC_NAME_1 = "test1";
    private static final String TEST_METRIC_NAME_2 = "test2";
    private static final String TEST_METRIC_NAME_3 = "test3";
    private static final String TEST_METRIC_NAME_NANO_1 = "test_nano_1";
    private static final String TEST_METRIC_NAME_NANO_2 = "test_nano_2";
    private static final String TEST_METRIC_NAME_NANO_3 = "test_nano_3";
    private static final String TEST_METRIC_NAME_BYTE = "test_byte";
    private static final long ONE_SECOND_IN_NANOS = 1_000_000_000L;

    private void assertRuntimeMetricEquals(RuntimeMetric m1, RuntimeMetric m2)
    {
        assertEquals(m1.getName(), m2.getName());
        assertEquals(m1.getUnit(), m2.getUnit());
        assertEquals(m1.getSum(), m2.getSum());
        assertEquals(m1.getCount(), m2.getCount());
        assertEquals(m1.getMax(), m2.getMax());
        assertEquals(m1.getMin(), m2.getMin());
        assertEquals(m1.getEvents(), m2.getEvents());
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
        QueryTracer queryTracer = stats.startQueryTrace();
        assertTrue(stats.isTracingEnabled());
        stats.recordWallTime(TEST_METRIC_NAME_NANO_2, () -> {});
        queryTracer.finishQueryTrace(false);

        JsonCodec<RuntimeStats> codec = JsonCodec.jsonCodec(RuntimeStats.class);
        String json = codec.toJson(stats);
        assertTrue(json.contains("\"events\""));
        RuntimeStats actual = codec.fromJson(json);

        actual.getMetrics().forEach((name, metric) -> assertRuntimeMetricEquals(metric, stats.getMetric(name)));
    }

    @Test
    public void testJsonWithoutTracingOmitsEvents()
    {
        RuntimeStats stats = new RuntimeStats();
        stats.addMetricValue(TEST_METRIC_NAME_1, NONE, 1);

        String json = JsonCodec.jsonCodec(RuntimeStats.class).toJson(stats);
        assertFalse(json.contains("\"events\""));

        RuntimeStats actual = JsonCodec.jsonCodec(RuntimeStats.class).fromJson(json);
        assertRuntimeMetricEquals(actual.getMetric(TEST_METRIC_NAME_1), stats.getMetric(TEST_METRIC_NAME_1));
    }

    @Test
    public void testTraceEventJsonWithoutThreadNames()
    {
        String json = "{" +
                "\"spanId\":2," +
                "\"parentSpanId\":1," +
                "\"startTimeNanos\":10," +
                "\"endTimeNanos\":20," +
                "\"durationNanos\":10," +
                "\"startThreadId\":3," +
                "\"endThreadId\":4," +
                "\"failed\":false}";
        RuntimeMetricEvent event = JsonCodec.jsonCodec(RuntimeMetricEvent.class).fromJson(json);

        assertEquals(event.getDurationNanos(), 10);
        assertNull(event.getStartThreadName());
        assertNull(event.getEndThreadName());
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

    @Test
    public void testRecordWallTime()
    {
        RuntimeStats stats = new RuntimeStats();

        assertEquals(stats.recordWallTime(TEST_METRIC_NAME_NANO_3, () -> 1), 1);
        assertThat(stats.getMetric(TEST_METRIC_NAME_NANO_3).getSum()).isLessThan(ONE_SECOND_IN_NANOS);

        stats.recordWallTime(TEST_METRIC_NAME_NANO_2, () -> {});
        assertThat(stats.getMetric(TEST_METRIC_NAME_NANO_2).getSum()).isLessThan(ONE_SECOND_IN_NANOS);
    }

    @Test
    public void testRuntimeStatsTracing()
    {
        RuntimeStats stats = new RuntimeStats();
        stats.recordWallTime(TEST_METRIC_NAME_NANO_1, () -> {});
        assertTrue(stats.getMetric(TEST_METRIC_NAME_NANO_1).getEvents().isEmpty());
        assertNull(stats.getQueryTracer());
        assertNull(stats.getMetric(QUERY_TRACE_TIME_NANOS));

        QueryTracer queryTracer = enableTracing(stats);
        queryTracer.startQueryTrace();
        stats.recordWallTime(TEST_METRIC_NAME_NANO_3, () ->
                stats.recordWallTime(TEST_METRIC_NAME_NANO_2, () -> {}));
        queryTracer.finishQueryTrace(false);
        queryTracer.finishQueryTrace(false);

        assertNull(stats.getQueryTracer());
        RuntimeMetricEvent event = stats.getMetric(TEST_METRIC_NAME_NANO_2).getEvents().get(0);
        RuntimeMetricEvent enclosingEvent = stats.getMetric(TEST_METRIC_NAME_NANO_3).getEvents().get(0);
        RuntimeMetricEvent queryEvent = stats.getMetric(QUERY_TRACE_TIME_NANOS).getEvents().get(0);
        assertTrue(event.getStartTimeNanos() > 0);
        assertTrue(event.getEndTimeNanos() >= event.getStartTimeNanos());
        assertEquals(event.getDurationNanos(), event.getEndTimeNanos() - event.getStartTimeNanos());
        assertEquals(event.getStartThreadId(), Thread.currentThread().getId());
        assertEquals(event.getEndThreadId(), Thread.currentThread().getId());
        assertEquals(event.getStartThreadName(), Thread.currentThread().getName());
        assertEquals(event.getEndThreadName(), Thread.currentThread().getName());
        assertFalse(event.isFailed());
        assertEquals(event.getParentSpanId(), enclosingEvent.getSpanId());
        assertEquals(enclosingEvent.getParentSpanId(), queryEvent.getSpanId());
        assertEquals(queryEvent.getParentSpanId(), 0);
        assertEquals(stats.getMetric(QUERY_TRACE_TIME_NANOS).getCount(), 1);
        assertTrue(queryEvent.getStartTimeNanos() <= enclosingEvent.getStartTimeNanos());
        assertTrue(queryEvent.getEndTimeNanos() >= enclosingEvent.getEndTimeNanos());
        assertTrue(enclosingEvent.getStartTimeNanos() <= event.getStartTimeNanos());
        assertTrue(enclosingEvent.getEndTimeNanos() >= event.getEndTimeNanos());
    }

    @Test
    public void testQueryTraceEventLimit()
    {
        RuntimeStats stats = new RuntimeStats();
        QueryTracer queryTracer = stats.startQueryTrace(3);

        queryTracer.startTraceSpan(TEST_METRIC_NAME_NANO_1).close();
        queryTracer.startTraceSpan(TEST_METRIC_NAME_NANO_2).close();
        queryTracer.startTraceSpan(TEST_METRIC_NAME_NANO_1).close();
        queryTracer.finishQueryTrace(false);

        assertEquals(stats.getMetric(TEST_METRIC_NAME_NANO_1).getCount(), 2);
        assertEquals(stats.getMetric(TEST_METRIC_NAME_NANO_2).getCount(), 1);
        assertEquals(stats.getMetric(TEST_METRIC_NAME_NANO_1).getEvents().size(), 1);
        assertEquals(stats.getMetric(TEST_METRIC_NAME_NANO_2).getEvents().size(), 1);
        assertEquals(stats.getMetric(QUERY_TRACE_TIME_NANOS).getEvents().size(), 1);
    }

    @Test
    public void testQueryTraceEventLimitMustBePositive()
    {
        RuntimeStats stats = new RuntimeStats();
        expectThrows(IllegalArgumentException.class, () -> stats.startQueryTrace(0));
    }

    @Test
    public void testTraceEventsAreCopiedAndMerged()
    {
        RuntimeStats stats = new RuntimeStats();
        QueryTracer queryTracer = enableTracing(stats);
        queryTracer.startQueryTrace();
        stats.recordWallTime(TEST_METRIC_NAME_NANO_1, () -> {});
        queryTracer.finishQueryTrace(false);

        RuntimeStats copiedStats = RuntimeStats.copyOf(stats);
        RuntimeStats mergedStats = new RuntimeStats();
        mergedStats.mergeWith(stats);

        assertRuntimeMetricEquals(copiedStats.getMetric(TEST_METRIC_NAME_NANO_1), stats.getMetric(TEST_METRIC_NAME_NANO_1));
        assertRuntimeMetricEquals(copiedStats.getMetric(QUERY_TRACE_TIME_NANOS), stats.getMetric(QUERY_TRACE_TIME_NANOS));
        assertRuntimeMetricEquals(mergedStats.getMetric(TEST_METRIC_NAME_NANO_1), stats.getMetric(TEST_METRIC_NAME_NANO_1));
        assertRuntimeMetricEquals(mergedStats.getMetric(QUERY_TRACE_TIME_NANOS), stats.getMetric(QUERY_TRACE_TIME_NANOS));
        assertNull(copiedStats.getQueryTracer());
        assertNull(mergedStats.getQueryTracer());
    }

    @Test
    public void testTracingDisabledByDefault()
    {
        RuntimeStats stats = new RuntimeStats();

        assertFalse(stats.isTracingEnabled());
        assertNull(stats.getQueryTracer());
    }

    @Test
    public void testTraceEventsRequireQueryTrace()
    {
        RuntimeStats stats = new RuntimeStats();
        QueryTracer queryTracer = enableTracing(stats);

        assertTrue(stats.isTracingEnabled());
        assertNull(stats.getQueryTracer());
        stats.recordWallTime(TEST_METRIC_NAME_NANO_1, () -> {});
        queryTracer.startTraceSpan(TEST_METRIC_NAME_NANO_2).close();

        assertTrue(stats.getMetric(TEST_METRIC_NAME_NANO_1).getEvents().isEmpty());
        assertNull(stats.getMetric(TEST_METRIC_NAME_NANO_2));
        assertNull(stats.getMetric(QUERY_TRACE_TIME_NANOS));
    }

    @Test
    public void testResetStartsFreshTrace()
    {
        RuntimeStats stats = new RuntimeStats();
        QueryTracer queryTracer = enableTracing(stats);
        queryTracer.startQueryTrace();
        QueryTracer.TraceSpan oldSpan = queryTracer.startTraceSpan(TEST_METRIC_NAME_NANO_1);

        stats.reset();
        queryTracer.startQueryTrace();
        stats.recordWallTime(TEST_METRIC_NAME_NANO_2, () -> {});
        oldSpan.close();
        queryTracer.finishQueryTrace(false);

        assertNull(stats.getMetric(TEST_METRIC_NAME_NANO_1));
        assertEquals(stats.getMetric(TEST_METRIC_NAME_NANO_2).getEvents().size(), 1);
        assertEquals(stats.getMetric(QUERY_TRACE_TIME_NANOS).getEvents().size(), 1);
    }

    @Test
    public void testRuntimeStatsTracingAcrossThreads()
    {
        RuntimeStats stats = new RuntimeStats();
        QueryTracer queryTracer = enableTracing(stats);
        queryTracer.startQueryTrace();
        ExecutorService executor = newSingleThreadExecutor();
        AtomicLong workerThreadId = new AtomicLong();
        AtomicReference<String> workerThreadName = new AtomicReference<>();

        try {
            stats.recordWallTime(TEST_METRIC_NAME_NANO_3, () ->
                    CompletableFuture.supplyAsync(queryTracer.wrapSupplierWithTraceContext(() -> {
                        workerThreadId.set(Thread.currentThread().getId());
                        workerThreadName.set(Thread.currentThread().getName());
                        stats.recordWallTime(TEST_METRIC_NAME_NANO_2, () -> {});
                        return null;
                    }), executor).join());
            CompletableFuture.runAsync(() -> queryTracer.finishQueryTrace(false), executor).join();
        }
        finally {
            executor.shutdownNow();
        }

        RuntimeMetricEvent event = stats.getMetric(TEST_METRIC_NAME_NANO_2).getEvents().get(0);
        RuntimeMetricEvent enclosingEvent = stats.getMetric(TEST_METRIC_NAME_NANO_3).getEvents().get(0);
        RuntimeMetricEvent queryEvent = stats.getMetric(QUERY_TRACE_TIME_NANOS).getEvents().get(0);
        assertEquals(event.getParentSpanId(), enclosingEvent.getSpanId());
        assertEquals(event.getStartThreadId(), workerThreadId.get());
        assertEquals(event.getEndThreadId(), workerThreadId.get());
        assertEquals(event.getStartThreadName(), workerThreadName.get());
        assertEquals(event.getEndThreadName(), workerThreadName.get());
        assertEquals(queryEvent.getStartThreadId(), Thread.currentThread().getId());
        assertEquals(queryEvent.getEndThreadId(), workerThreadId.get());
        assertEquals(queryEvent.getStartThreadName(), Thread.currentThread().getName());
        assertEquals(queryEvent.getEndThreadName(), workerThreadName.get());
    }

    @Test
    public void testCallableTraceContextAcrossThreads()
    {
        RuntimeStats stats = new RuntimeStats();
        QueryTracer queryTracer = enableTracing(stats);
        queryTracer.startQueryTrace();
        ExecutorService executor = newSingleThreadExecutor();

        try {
            stats.recordWallTime(TEST_METRIC_NAME_NANO_3, () -> {
                Callable<Void> callable = queryTracer.wrapCallableWithTraceContext(() -> {
                    stats.recordWallTime(TEST_METRIC_NAME_NANO_2, () -> {});
                    return null;
                });
                CompletableFuture.supplyAsync(() -> {
                    try {
                        return callable.call();
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }, executor).join();
            });
        }
        finally {
            executor.shutdownNow();
        }
        queryTracer.finishQueryTrace(false);

        RuntimeMetricEvent event = stats.getMetric(TEST_METRIC_NAME_NANO_2).getEvents().get(0);
        RuntimeMetricEvent enclosingEvent = stats.getMetric(TEST_METRIC_NAME_NANO_3).getEvents().get(0);
        assertEquals(event.getParentSpanId(), enclosingEvent.getSpanId());
    }

    @Test
    public void testTraceSpanAcrossThreads()
    {
        RuntimeStats stats = new RuntimeStats();
        QueryTracer queryTracer = enableTracing(stats);
        queryTracer.startQueryTrace();
        QueryTracer.TraceSpan traceSpan = queryTracer.startTraceSpan(TEST_METRIC_NAME_NANO_1);
        ExecutorService executor = newSingleThreadExecutor();
        AtomicLong workerThreadId = new AtomicLong();
        AtomicReference<String> workerThreadName = new AtomicReference<>();

        try {
            CompletableFuture.runAsync(() -> {
                workerThreadId.set(Thread.currentThread().getId());
                workerThreadName.set(Thread.currentThread().getName());
                traceSpan.close();
                traceSpan.close();
            }, executor).join();
        }
        finally {
            executor.shutdownNow();
        }
        queryTracer.finishQueryTrace(false);

        RuntimeMetric metric = stats.getMetric(TEST_METRIC_NAME_NANO_1);
        RuntimeMetricEvent event = metric.getEvents().get(0);
        RuntimeMetricEvent queryEvent = stats.getMetric(QUERY_TRACE_TIME_NANOS).getEvents().get(0);
        assertEquals(metric.getCount(), 1);
        assertEquals(event.getParentSpanId(), queryEvent.getSpanId());
        assertEquals(event.getStartThreadId(), Thread.currentThread().getId());
        assertEquals(event.getEndThreadId(), workerThreadId.get());
        assertEquals(event.getStartThreadName(), Thread.currentThread().getName());
        assertEquals(event.getEndThreadName(), workerThreadName.get());
        assertFalse(event.isFailed());
    }

    @Test
    public void testCompletedTraceEvent()
    {
        RuntimeStats stats = new RuntimeStats();
        QueryTracer queryTracer = enableTracing(stats);
        queryTracer.startQueryTrace();
        long startTimeNanos = System.nanoTime();

        queryTracer.recordTraceEvent(TEST_METRIC_NAME_NANO_1, startTimeNanos, startTimeNanos + 123, true);
        queryTracer.finishQueryTrace(false);

        RuntimeMetricEvent event = stats.getMetric(TEST_METRIC_NAME_NANO_1).getEvents().get(0);
        assertEquals(event.getDurationNanos(), 123);
        assertEquals(event.getStartThreadId(), -1);
        assertNull(event.getStartThreadName());
        assertEquals(event.getEndThreadId(), Thread.currentThread().getId());
        assertEquals(event.getEndThreadName(), Thread.currentThread().getName());
        assertTrue(event.isFailed());
    }

    @Test
    public void testCompletedTraceEventWithStartThread()
    {
        RuntimeStats stats = new RuntimeStats();
        QueryTracer queryTracer = enableTracing(stats);
        queryTracer.startQueryTrace();
        long startTimeNanos = System.nanoTime();

        queryTracer.recordTraceEvent(TEST_METRIC_NAME_NANO_1, startTimeNanos, startTimeNanos + 123, 17, "stage-transition-thread", false);
        queryTracer.finishQueryTrace(false);

        RuntimeMetricEvent event = stats.getMetric(TEST_METRIC_NAME_NANO_1).getEvents().get(0);
        assertEquals(event.getStartThreadId(), 17);
        assertEquals(event.getStartThreadName(), "stage-transition-thread");
        assertEquals(event.getEndThreadId(), Thread.currentThread().getId());
        assertEquals(event.getEndThreadName(), Thread.currentThread().getName());
    }

    @Test
    public void testCompletedTraceEventClampsNegativeDuration()
    {
        RuntimeStats stats = new RuntimeStats();
        QueryTracer queryTracer = enableTracing(stats);
        queryTracer.startQueryTrace();
        long startTimeNanos = System.nanoTime();

        queryTracer.recordTraceEvent(TEST_METRIC_NAME_NANO_1, startTimeNanos, startTimeNanos - 1);
        queryTracer.finishQueryTrace(false);

        RuntimeMetricEvent event = stats.getMetric(TEST_METRIC_NAME_NANO_1).getEvents().get(0);
        assertEquals(event.getStartTimeNanos(), event.getEndTimeNanos());
        assertEquals(event.getDurationNanos(), 0);

        queryTracer.recordTraceEvent(TEST_METRIC_NAME_NANO_2, startTimeNanos, startTimeNanos - 1);
        assertNull(stats.getMetric(TEST_METRIC_NAME_NANO_2));
    }

    @Test
    public void testCompletedTraceEventBeforeQueryTraceIsIgnored()
    {
        long startTimeNanos = System.nanoTime() - MILLISECONDS.toNanos(1);
        RuntimeStats stats = new RuntimeStats();
        QueryTracer queryTracer = enableTracing(stats);
        queryTracer.startQueryTrace();

        queryTracer.recordTraceEvent(TEST_METRIC_NAME_NANO_1, startTimeNanos, System.nanoTime());
        queryTracer.finishQueryTrace(false);

        assertNull(stats.getMetric(TEST_METRIC_NAME_NANO_1));
    }

    @Test
    public void testFailedRuntimeStatsCallProducesTraceEvent()
    {
        RuntimeStats stats = new RuntimeStats();
        QueryTracer queryTracer = enableTracing(stats);
        queryTracer.startQueryTrace();

        expectThrows(IllegalStateException.class, () -> stats.recordWallTime(TEST_METRIC_NAME_NANO_1, () -> {
            throw new IllegalStateException("test failure");
        }));
        queryTracer.finishQueryTrace(false);

        RuntimeMetric metric = stats.getMetric(TEST_METRIC_NAME_NANO_1);
        assertEquals(metric.getCount(), 0);
        assertTrue(metric.getEvents().get(0).isFailed());
    }

    private static QueryTracer enableTracing(RuntimeStats stats)
    {
        return stats.enableTracing();
    }

    @Test
    public void testRecordWallAndCpuTime()
    {
        RuntimeStats stats = new RuntimeStats();

        assertEquals(stats.recordWallAndCpuTime(TEST_METRIC_NAME_NANO_1, () -> {
            sleepUninterruptibly(100, MILLISECONDS);
            return 1;
        }), 1);
        assertThat(stats.getMetric(TEST_METRIC_NAME_NANO_1).getSum()).isGreaterThanOrEqualTo(MILLISECONDS.toNanos(100));
        assertThat(stats.getMetric(TEST_METRIC_NAME_NANO_1 + "OnCpu").getSum()).isLessThan(MILLISECONDS.toNanos(100));

        stats.recordWallAndCpuTime(TEST_METRIC_NAME_NANO_2, () -> sleepUninterruptibly(100, MILLISECONDS));
        assertThat(stats.getMetric(TEST_METRIC_NAME_NANO_2).getSum()).isGreaterThanOrEqualTo(MILLISECONDS.toNanos(100));
        assertThat(stats.getMetric(TEST_METRIC_NAME_NANO_2 + "OnCpu").getSum()).isLessThan(MILLISECONDS.toNanos(100));
    }
}
