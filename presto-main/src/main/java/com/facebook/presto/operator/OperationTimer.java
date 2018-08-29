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
package com.facebook.presto.operator;

import com.google.common.base.Ticker;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class OperationTimer
{
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();
    private static final long DEFAULT_MIN_NANOS_FOR_CPU_TIMING = new Duration(250, TimeUnit.MILLISECONDS).roundTo(NANOSECONDS);

    private final Ticker ticker;
    private final long minNanosForCpuTiming;

    private final long wallStart;
    private final long cpuStart;
    private final long userStart;

    private long intervalWallStart;

    private long intervalUnaccountedCpuWallTime;
    private long intervalCpuStart;
    private long intervalUserStart;

    private final Map<OperationTiming, InternalTiming> timings = new HashMap<>();

    public OperationTimer()
    {
        this(Ticker.systemTicker(), DEFAULT_MIN_NANOS_FOR_CPU_TIMING);
    }

    public OperationTimer(Ticker ticker, long minNanosForCpuTiming)
    {
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.minNanosForCpuTiming = minNanosForCpuTiming;

        wallStart = ticker.read();
        cpuStart = currentThreadCpuTime();
        userStart = currentThreadUserTime();

        intervalWallStart = wallStart;
        intervalCpuStart = cpuStart;
        intervalUserStart = userStart;
    }

    public void recordOperationComplete(OperationTiming operationTiming)
    {
        requireNonNull(operationTiming, "operationTiming is null");
        InternalTiming internalTiming = timings.computeIfAbsent(operationTiming, InternalTiming::new);

        long intervalWallEnd = ticker.read();
        long intervalWallDuration = max(0, intervalWallEnd - intervalWallStart);
        // only measure cpu time if the wall interval is large
        if (intervalWallDuration > minNanosForCpuTiming) {
            long intervalCpuEnd = currentThreadCpuTime();
            long intervalCpuDuration = max(0, intervalCpuEnd - intervalCpuStart - intervalUnaccountedCpuWallTime);

            long intervalUserEnd = currentThreadUserTime();
            long intervalUserDuration = max(0, intervalUserEnd - intervalUserStart - intervalUnaccountedCpuWallTime);

            internalTiming.recordMeasuredCpuTime(intervalWallDuration, intervalCpuDuration, intervalUserDuration);

            intervalUnaccountedCpuWallTime = 0;
            intervalWallStart = intervalWallEnd;
            intervalCpuStart = intervalCpuEnd;
            intervalUserStart = intervalUserEnd;
        }
        else {
            internalTiming.recordUnaccountedCpuTime(intervalWallDuration);

            intervalUnaccountedCpuWallTime += intervalWallDuration;
            intervalWallStart = intervalWallEnd;
        }
    }

    public void end(OperationTiming overallTiming)
    {
        if (overallTiming == null && timings.isEmpty()) {
            return;
        }

        long wallEnd = ticker.read();
        long cpuEnd = currentThreadCpuTime();
        long userEnd = currentThreadUserTime();

        if (overallTiming != null) {
            long wallDuration = wallEnd - wallStart;
            overallTiming.recordWallTime(wallDuration);
            if (cpuEnd > cpuStart) {
                overallTiming.recordEstimatedCpuTime(cpuEnd - cpuStart);
                overallTiming.recordEstimatedUserTime(userEnd - userStart);
            }
            else {
                overallTiming.recordEstimatedCpuTime(wallDuration);
                overallTiming.recordEstimatedUserTime(wallDuration);
            }
        }

        long totalUnaccountedCpuWallNanos = timings.values().stream()
                .mapToLong(InternalTiming::getCpuUnaccountedWallNanos)
                .sum();

        long totalMeasuredCpuNanos = timings.values().stream()
                .mapToLong(InternalTiming::getCpuMeasuredNanos)
                .sum();
        long totalUnaccountedCpuNanos = max(0, cpuEnd - cpuStart - totalMeasuredCpuNanos);

        long totalMeasuredUserNanos = timings.values().stream()
                .mapToLong(InternalTiming::getUserMeasuredNanos)
                .sum();
        long totalUnaccountedUserNanos = max(0, userEnd - userStart - totalMeasuredUserNanos);

        for (InternalTiming timing : timings.values()) {
            timing.recordTimings(totalUnaccountedCpuNanos, totalUnaccountedUserNanos, totalUnaccountedCpuWallNanos);
        }
    }

    private static long currentThreadUserTime()
    {
        return THREAD_MX_BEAN.getCurrentThreadUserTime();
    }

    private static long currentThreadCpuTime()
    {
        return THREAD_MX_BEAN.getCurrentThreadCpuTime();
    }

    private static class InternalTiming
    {
        private final OperationTiming operationTiming;

        private long cpuMeasuredNanos;
        private long userMeasuredNanos;
        private long cpuUnaccountedWallNanos;

        public InternalTiming(OperationTiming operationTiming)
        {
            this.operationTiming = operationTiming;
        }

        public void recordMeasuredCpuTime(long intervalWallNanos, long intervalCpuNanos, long intervalUserNanos)
        {
            operationTiming.recordWallTime(intervalWallNanos);

            cpuMeasuredNanos += intervalCpuNanos;
            userMeasuredNanos += intervalUserNanos;
        }

        public void recordUnaccountedCpuTime(long intervalWallNanos)
        {
            operationTiming.recordWallTime(intervalWallNanos);
            cpuUnaccountedWallNanos += intervalWallNanos;
        }

        public long getCpuMeasuredNanos()
        {
            return cpuMeasuredNanos;
        }

        public long getCpuUnaccountedWallNanos()
        {
            return cpuUnaccountedWallNanos;
        }

        public long getUserMeasuredNanos()
        {
            return userMeasuredNanos;
        }

        public void recordTimings(long totalUnaccountedCpuNanos, long totalUnaccountedUserNanos, long totalUnaccountedCpuWallNanos)
        {
            if (cpuUnaccountedWallNanos > 0) {
                double unaccountedCpuFraction = cpuUnaccountedWallNanos / (double) totalUnaccountedCpuWallNanos;

                // measured CPU + this timers portion of the unaccounted CPU
                operationTiming.recordEstimatedCpuTime((long) (cpuMeasuredNanos + totalUnaccountedCpuNanos * unaccountedCpuFraction));
                // measured USER + this timers portion of the unaccounted USER
                operationTiming.recordEstimatedUserTime((long) (userMeasuredNanos + totalUnaccountedUserNanos * unaccountedCpuFraction));
            }
            else {
                operationTiming.recordEstimatedCpuTime(cpuMeasuredNanos);
                operationTiming.recordEstimatedUserTime(userMeasuredNanos);
            }
        }
    }

    @ThreadSafe
    public static class OperationTiming
    {
        private final AtomicLong calls = new AtomicLong();
        private final AtomicLong wallNanos = new AtomicLong();
        private final AtomicLong estimatedCpuNanos = new AtomicLong();
        private final AtomicLong estimatedUserNanos = new AtomicLong();

        public long getCalls()
        {
            return calls.get();
        }

        public long getWallNanos()
        {
            return wallNanos.get();
        }

        public long getEstimatedCpuNanos()
        {
            return estimatedCpuNanos.get();
        }

        public long getEstimatedUserNanos()
        {
            return estimatedUserNanos.get();
        }

        void recordWallTime(long wallNanos)
        {
            this.calls.incrementAndGet();
            this.wallNanos.addAndGet(wallNanos);
        }

        void recordEstimatedCpuTime(long estimatedCpuNanos)
        {
            this.estimatedCpuNanos.addAndGet(estimatedCpuNanos);
        }

        void recordEstimatedUserTime(long estimatedUserNanos)
        {
            this.estimatedUserNanos.addAndGet(estimatedUserNanos);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("calls", calls)
                    .add("wallNanos", wallNanos)
                    .add("estimatedCpuNanos", estimatedCpuNanos)
                    .add("estimatedUserNanos", estimatedUserNanos)
                    .toString();
        }
    }
}
