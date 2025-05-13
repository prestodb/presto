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

import com.sun.management.ThreadMXBean;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
class OperationTimer
{
    private static final ThreadMXBean THREAD_MX_BEAN = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    private final boolean trackOverallCpuTime;
    private final boolean trackOperationCpuTime;

    private final boolean trackOverallAllocation;
    private final boolean trackOperationAllocation;

    private final long wallStart;
    private final long cpuStart;
    private final long allocationStart;

    private long intervalWallStart;
    private long intervalCpuStart;
    private long intervalAllocationStart;

    private boolean finished;

    OperationTimer(boolean trackOverallCpuTime)
    {
        this(trackOverallCpuTime, false, false, false);
    }

    OperationTimer(boolean trackOverallCpuTime, boolean trackOperationCpuTime, boolean trackOverallAllocation, boolean trackOperationAllocation)
    {
        this.trackOverallCpuTime = trackOverallCpuTime;
        this.trackOperationCpuTime = trackOperationCpuTime;
        checkArgument(trackOverallCpuTime || !trackOperationCpuTime, "tracking operation cpu time without tracking overall cpu time is not supported");

        this.trackOverallAllocation = trackOverallAllocation;
        this.trackOperationAllocation = trackOperationAllocation;
        checkArgument(trackOverallAllocation || !trackOperationAllocation, "tracking operation allocation without tracking overall allocation is not supported");

        wallStart = System.nanoTime();
        cpuStart = trackOverallCpuTime ? currentThreadCpuTime() : 0;
        allocationStart = trackOverallAllocation ? currentThreadAllocation() : 0;

        intervalWallStart = wallStart;
        intervalCpuStart = cpuStart;
        intervalAllocationStart = allocationStart;
    }

    void recordOperationComplete(OperationTiming operationTiming)
    {
        requireNonNull(operationTiming, "operationTiming is null");
        checkState(!finished, "timer is finished");

        long intervalCpuEnd = trackOperationCpuTime ? currentThreadCpuTime() : 0;
        long intervalWallEnd = System.nanoTime();
        long intervalAllocationEnd = trackOperationAllocation ? currentThreadAllocation() : 0;

        long operationWallNanos = nanosBetween(intervalWallStart, intervalWallEnd);
        long operationCpuNanos = trackOperationCpuTime ? nanosBetween(intervalCpuStart, intervalCpuEnd) : 0;
        long operationAllocation = trackOperationAllocation ? bytesBetween(intervalAllocationStart, intervalAllocationEnd) : 0;
        operationTiming.record(operationWallNanos, operationCpuNanos, operationAllocation);

        intervalWallStart = intervalWallEnd;
        intervalCpuStart = intervalCpuEnd;
        intervalAllocationStart = intervalAllocationEnd;
    }

    void end(OperationTiming overallTiming)
    {
        requireNonNull(overallTiming, "overallTiming is null");
        checkState(!finished, "timer is finished");
        finished = true;

        long cpuEnd = trackOverallCpuTime ? currentThreadCpuTime() : 0;
        long wallEnd = System.nanoTime();
        long allocationEnd = trackOverallAllocation ? currentThreadAllocation() : 0;

        overallTiming.record(nanosBetween(wallStart, wallEnd), nanosBetween(cpuStart, cpuEnd), bytesBetween(allocationStart, allocationEnd));
    }

    private static long currentThreadCpuTime()
    {
        return THREAD_MX_BEAN.getCurrentThreadCpuTime();
    }

    private static long currentThreadAllocation()
    {
        return THREAD_MX_BEAN.getThreadAllocatedBytes(Thread.currentThread().getId());
    }

    private static long nanosBetween(long start, long end)
    {
        return max(0, end - start);
    }

    private static long bytesBetween(long start, long end)
    {
        return max(0, end - start);
    }

    @ThreadSafe
    static class OperationTiming
    {
        private final AtomicLong calls = new AtomicLong();
        private final AtomicLong wallNanos = new AtomicLong();
        private final AtomicLong cpuNanos = new AtomicLong();
        private final AtomicLong allocationBytes = new AtomicLong();

        long getCalls()
        {
            return calls.get();
        }

        long getWallNanos()
        {
            return wallNanos.get();
        }

        long getCpuNanos()
        {
            return cpuNanos.get();
        }

        long getAllocationBytes()
        {
            return allocationBytes.get();
        }

        void record(long wallNanos, long cpuNanos, long allocationBytes)
        {
            this.calls.incrementAndGet();
            this.wallNanos.addAndGet(wallNanos);
            this.cpuNanos.addAndGet(cpuNanos);
            this.allocationBytes.addAndGet(allocationBytes);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("calls", calls)
                    .add("wallNanos", wallNanos)
                    .add("cpuNanos", cpuNanos)
                    .add("allocationBytes", allocationBytes)
                    .toString();
        }
    }
}
