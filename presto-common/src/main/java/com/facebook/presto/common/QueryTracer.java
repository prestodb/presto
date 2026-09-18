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

import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import jakarta.annotation.Nullable;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.facebook.presto.common.RuntimeMetricEvent.UNKNOWN_THREAD_ID;
import static com.facebook.presto.common.RuntimeMetricName.QUERY_TRACE_TIME_NANOS;
import static com.facebook.presto.common.RuntimeUnit.NANO;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Records query trace spans as events in the RuntimeStats instance that created this tracer.
 * The event limit includes the query root event and does not limit aggregate metric updates.
 */
@ThreadSafe
public final class QueryTracer
{
    private final ConcurrentMap<String, RuntimeMetric> metrics;
    private final int maxEvents;
    private final ThreadLocal<TraceParent> currentTraceParent = new ThreadLocal<>();

    private volatile TraceState traceState;

    QueryTracer(ConcurrentMap<String, RuntimeMetric> metrics, int maxEvents)
    {
        this.metrics = requireNonNull(metrics, "metrics is null");
        checkArgument(maxEvents > 0, "maxEvents must be greater than zero");
        this.maxEvents = maxEvents;
    }

    synchronized void reset()
    {
        TraceState state = traceState;
        if (state != null) {
            synchronized (state) {
                state.finished = true;
                traceState = null;
            }
        }
        currentTraceParent.remove();
    }

    boolean isTracingActive()
    {
        TraceState state = traceState;
        return state != null && !state.finished;
    }

    /**
     * Starts the query root span if it has not already been started.
     */
    synchronized void startQueryTrace()
    {
        if (traceState != null) {
            return;
        }
        long epochStartNanos = MILLISECONDS.toNanos(System.currentTimeMillis());
        long monotonicStartNanos = System.nanoTime();
        traceState = new TraceState(
                epochStartNanos,
                monotonicStartNanos,
                Thread.currentThread().getId(),
                Thread.currentThread().getName());
    }

    /**
     * Completes the active query root span. Subsequent span completions are ignored.
     */
    public synchronized void finishQueryTrace(boolean failed)
    {
        TraceState state = traceState;
        if (state != null) {
            finishRootTrace(state, System.nanoTime(), failed);
        }
    }

    /**
     * Starts a span that may be completed from a different thread. Unlike RuntimeStats timing
     * methods, this does not install the span as the current thread's parent.
     */
    public TraceSpan startTraceSpan(String tag)
    {
        requireNonNull(tag, "tag is null");
        TraceContext traceContext = createTraceContext(System.nanoTime());
        if (traceContext == null) {
            if (isTracingActive()) {
                long startTimeNanos = System.nanoTime();
                return new TraceSpan((endTimeNanos, failed) -> addMetricValue(tag, NANO, endTimeNanos - startTimeNanos));
            }
            return TraceSpan.noop();
        }
        return new TraceSpan((endTimeNanos, failed) -> finishTraceSpan(tag, traceContext, endTimeNanos, failed));
    }

    /**
     * Records a completed interval measured with {@link System#nanoTime()}.
     */
    public void recordTraceEvent(String tag, long startTimeNanos, long endTimeNanos)
    {
        recordTraceEvent(tag, startTimeNanos, endTimeNanos, false);
    }

    /**
     * Records a completed interval measured with {@link System#nanoTime()} without changing the aggregate value.
     * The event is attached to the query root span, and its start thread is unknown.
     * Intervals that begin before the query root are ignored.
     */
    public void recordTraceEvent(String tag, long startTimeNanos, long endTimeNanos, boolean failed)
    {
        recordTraceEvent(tag, startTimeNanos, endTimeNanos, UNKNOWN_THREAD_ID, null, failed);
    }

    /**
     * Records a completed interval measured with {@link System#nanoTime()} and the thread that
     * started it. The event is attached to the query root span, and the current thread is recorded
     * as the end thread. Intervals that begin before the query root are ignored.
     */
    public void recordTraceEvent(
            String tag,
            long startTimeNanos,
            long endTimeNanos,
            long startThreadId,
            @Nullable String startThreadName,
            boolean failed)
    {
        requireNonNull(tag, "tag is null");

        TraceContext traceContext = createTraceContext(startTimeNanos, false, startThreadId, startThreadName);
        if (traceContext != null) {
            addTraceEvent(tag, traceContext, Math.max(endTimeNanos, startTimeNanos), failed);
        }
    }

    TraceSpan startScopedTrace(String tag, long startTimeNanos)
    {
        TraceContext traceContext = createTraceContext(startTimeNanos);
        if (traceContext == null) {
            return TraceSpan.noop();
        }
        TraceState state = traceContext.traceState;
        TraceParent installedTraceParent;
        synchronized (state) {
            if (!isActive(state)) {
                return TraceSpan.noop();
            }
            installedTraceParent = new TraceParent(state, traceContext.spanId);
            currentTraceParent.set(installedTraceParent);
        }
        return new TraceSpan((endTimeNanos, failed) -> recordScopedTraceEvent(tag, traceContext, installedTraceParent, endTimeNanos, failed));
    }

    /**
     * Captures the current trace parent and restores it while the callable executes.
     */
    public <V> Callable<V> wrapCallableWithTraceContext(Callable<V> callable)
    {
        requireNonNull(callable, "callable is null");
        TraceParent traceParent = captureTraceParent();
        if (traceParent == null) {
            return callable;
        }
        return () -> callWithTraceParent(traceParent, callable);
    }

    /**
     * Captures the current trace parent and restores it while the supplier executes.
     */
    public <V> Supplier<V> wrapSupplierWithTraceContext(Supplier<V> supplier)
    {
        requireNonNull(supplier, "supplier is null");
        TraceParent traceParent = captureTraceParent();
        if (traceParent == null) {
            return supplier;
        }
        return () -> supplyWithTraceParent(traceParent, supplier);
    }

    private TraceContext createTraceContext(long startTimeNanos)
    {
        return createTraceContext(
                startTimeNanos,
                true,
                Thread.currentThread().getId(),
                Thread.currentThread().getName());
    }

    private TraceContext createTraceContext(
            long startTimeNanos,
            boolean useCurrentParent,
            long startThreadId,
            @Nullable String startThreadName)
    {
        TraceState state = traceState;
        if (state == null) {
            return null;
        }

        synchronized (state) {
            if (!isActive(state) || startTimeNanos < state.monotonicStartNanos) {
                return null;
            }
            if (state.recordedEventCount >= maxEvents - 1) {
                return null;
            }
            TraceParent previousTraceParent = useCurrentParent ? currentTraceParent.get() : null;
            long parentSpanId = previousTraceParent != null && previousTraceParent.traceState == state ? previousTraceParent.spanId : state.rootSpanId;
            long spanId = state.nextSpanId++;
            return new TraceContext(
                    state,
                    spanId,
                    parentSpanId,
                    state.epochStartNanos + (startTimeNanos - state.monotonicStartNanos),
                    startTimeNanos,
                    startThreadId,
                    startThreadName,
                    previousTraceParent);
        }
    }

    private void recordScopedTraceEvent(String tag, TraceContext traceContext, TraceParent installedTraceParent, long endTimeNanos, boolean failed)
    {
        try {
            addTraceEvent(tag, traceContext, endTimeNanos, failed);
        }
        finally {
            restoreTraceParent(traceContext.traceState, installedTraceParent, traceContext.previousTraceParent);
        }
    }

    private void finishTraceSpan(String tag, TraceContext traceContext, long endTimeNanos, boolean failed)
    {
        TraceState state = traceContext.traceState;
        synchronized (state) {
            if (!isActive(state)) {
                return;
            }
            addMetricValue(tag, NANO, endTimeNanos - traceContext.startTimeNanos);
            addTraceEventUnchecked(tag, traceContext, endTimeNanos, failed);
        }
    }

    private void addTraceEvent(String tag, TraceContext traceContext, long endTimeNanos, boolean failed)
    {
        TraceState state = traceContext.traceState;
        synchronized (state) {
            if (!isActive(state)) {
                return;
            }
            addTraceEventUnchecked(tag, traceContext, endTimeNanos, failed);
        }
    }

    private void addTraceEventUnchecked(String tag, TraceContext traceContext, long endTimeNanos, boolean failed)
    {
        TraceState state = traceContext.traceState;
        // Reserve one event for the query root so a capped trace retains its lifecycle boundary.
        if (state.recordedEventCount >= maxEvents - 1) {
            return;
        }
        state.recordedEventCount++;

        long durationNanos = endTimeNanos - traceContext.startTimeNanos;
        metrics.computeIfAbsent(tag, key -> new RuntimeMetric(tag, NANO)).addEvent(new RuntimeMetricEvent(
                traceContext.spanId,
                traceContext.parentSpanId,
                traceContext.startTimeEpochNanos,
                traceContext.startTimeEpochNanos + durationNanos,
                traceContext.startThreadId,
                Thread.currentThread().getId(),
                traceContext.startThreadName,
                Thread.currentThread().getName(),
                failed));
    }

    private TraceParent captureTraceParent()
    {
        TraceState state = traceState;
        if (state == null) {
            return null;
        }
        synchronized (state) {
            if (!isActive(state)) {
                return null;
            }
            TraceParent traceParent = currentTraceParent.get();
            if (traceParent != null && traceParent.traceState == state) {
                return traceParent;
            }
            return state.rootTraceParent;
        }
    }

    private <V> V callWithTraceParent(TraceParent traceParent, Callable<V> callable)
            throws Exception
    {
        if (!isActive(traceParent.traceState)) {
            return callable.call();
        }
        TraceParent previousTraceParent = currentTraceParent.get();
        currentTraceParent.set(traceParent);
        try {
            return callable.call();
        }
        finally {
            restoreTraceParent(traceParent.traceState, traceParent, previousTraceParent);
        }
    }

    private <V> V supplyWithTraceParent(TraceParent traceParent, Supplier<V> supplier)
    {
        if (!isActive(traceParent.traceState)) {
            return supplier.get();
        }
        TraceParent previousTraceParent = currentTraceParent.get();
        currentTraceParent.set(traceParent);
        try {
            return supplier.get();
        }
        finally {
            restoreTraceParent(traceParent.traceState, traceParent, previousTraceParent);
        }
    }

    private void restoreTraceParent(TraceState state, TraceParent installedTraceParent, TraceParent traceParent)
    {
        if (currentTraceParent.get() != installedTraceParent) {
            return;
        }
        if (traceParent == null || traceParent.traceState != state || !isActive(state)) {
            currentTraceParent.remove();
            return;
        }
        currentTraceParent.set(traceParent);
    }

    private void finishRootTrace(TraceState state, long endTimeNanos, boolean failed)
    {
        synchronized (state) {
            if (traceState != state || state.finished) {
                return;
            }
            state.finished = true;
            currentTraceParent.remove();

            // A concurrent span may have captured its end time before waiting for this lock.
            // Sample again so the root never ends before a span that was accepted into the trace.
            long traceEndTimeNanos = Math.max(endTimeNanos, System.nanoTime());
            long durationNanos = traceEndTimeNanos - state.monotonicStartNanos;
            addMetricValue(QUERY_TRACE_TIME_NANOS, NANO, durationNanos);
            metrics.computeIfAbsent(QUERY_TRACE_TIME_NANOS, key -> new RuntimeMetric(QUERY_TRACE_TIME_NANOS, NANO)).addEvent(new RuntimeMetricEvent(
                    state.rootSpanId,
                    0,
                    state.epochStartNanos,
                    state.epochStartNanos + durationNanos,
                    state.startThreadId,
                    Thread.currentThread().getId(),
                    state.startThreadName,
                    Thread.currentThread().getName(),
                    failed));
            traceState = null;
        }
    }

    private boolean isActive(TraceState state)
    {
        return traceState == state && !state.finished;
    }

    private void addMetricValue(String name, RuntimeUnit unit, long value)
    {
        metrics.computeIfAbsent(name, key -> new RuntimeMetric(name, unit)).addValue(value);
    }

    /**
     * One-shot handle for completing an in-progress trace span. Completion records an immutable
     * {@link RuntimeMetricEvent} through the owning tracer.
     */
    @ThreadSafe
    public static final class TraceSpan
            implements AutoCloseable
    {
        private static final TraceSpan NOOP = new TraceSpan();

        @Nullable
        private final AtomicReference<FinishCallback> finishCallback;

        private TraceSpan()
        {
            this.finishCallback = null;
        }

        private TraceSpan(FinishCallback finishCallback)
        {
            this.finishCallback = new AtomicReference<>(requireNonNull(finishCallback, "finishCallback is null"));
        }

        private static TraceSpan noop()
        {
            return NOOP;
        }

        @Override
        public void close()
        {
            if (finishCallback != null) {
                finish(System.nanoTime(), false);
            }
        }

        public void fail()
        {
            if (finishCallback != null) {
                finish(System.nanoTime(), true);
            }
        }

        void finish(long endTimeNanos, boolean failed)
        {
            if (finishCallback == null) {
                return;
            }
            FinishCallback callback = finishCallback.getAndSet(null);
            if (callback != null) {
                callback.finish(endTimeNanos, failed);
            }
        }

        @FunctionalInterface
        private interface FinishCallback
        {
            void finish(long endTimeNanos, boolean failed);
        }
    }

    private static final class TraceState
    {
        private static final long ROOT_SPAN_ID = 1;

        private final long rootSpanId = ROOT_SPAN_ID;
        private final long epochStartNanos;
        private final long monotonicStartNanos;
        private final long startThreadId;
        private final String startThreadName;
        private final TraceParent rootTraceParent = new TraceParent(this, ROOT_SPAN_ID);

        @GuardedBy("this")
        private long nextSpanId = ROOT_SPAN_ID + 1;
        @GuardedBy("this")
        private int recordedEventCount;
        private volatile boolean finished;

        private TraceState(long epochStartNanos, long monotonicStartNanos, long startThreadId, String startThreadName)
        {
            this.epochStartNanos = epochStartNanos;
            this.monotonicStartNanos = monotonicStartNanos;
            this.startThreadId = startThreadId;
            this.startThreadName = startThreadName;
        }
    }

    private static final class TraceParent
    {
        private final TraceState traceState;
        private final long spanId;

        private TraceParent(TraceState traceState, long spanId)
        {
            this.traceState = traceState;
            this.spanId = spanId;
        }
    }

    private static final class TraceContext
    {
        private final TraceState traceState;
        private final long spanId;
        private final long parentSpanId;
        private final long startTimeEpochNanos;
        private final long startTimeNanos;
        private final long startThreadId;
        @Nullable
        private final String startThreadName;
        @Nullable
        private final TraceParent previousTraceParent;

        private TraceContext(
                TraceState traceState,
                long spanId,
                long parentSpanId,
                long startTimeEpochNanos,
                long startTimeNanos,
                long startThreadId,
                @Nullable String startThreadName,
                @Nullable TraceParent previousTraceParent)
        {
            this.traceState = traceState;
            this.spanId = spanId;
            this.parentSpanId = parentSpanId;
            this.startTimeEpochNanos = startTimeEpochNanos;
            this.startTimeNanos = startTimeNanos;
            this.startThreadId = startThreadId;
            this.startThreadName = startThreadName;
            this.previousTraceParent = previousTraceParent;
        }
    }
}
