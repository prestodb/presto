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
package com.facebook.presto.execution;

import com.facebook.presto.operator.OperatorBlockedReason;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.GuardedBy;
import javax.validation.constraints.NotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class DriverProcessStats
{
    @GuardedBy("this")
    private long numProcessInternalCalls;
    @GuardedBy("this")
    private long totalNumPagesMoved;
    @GuardedBy("this")
    private long totalNumCachedPagesUsed;
    @GuardedBy("this")
    private long totalNumBlockedProcessInternalCount;
    @GuardedBy("this")
    private long totalNumBlockedProcessCount;
    @GuardedBy("this")
    private Map<OperatorBlockedReason, AtomicLong> firstBlockedReasons = new HashMap<>();
    @GuardedBy("this")
    private long numIterationsInProcess;
    @GuardedBy("this")
    private long nanosInProcess;
    @GuardedBy("this")
    private long numProcessCalls;

    public DriverProcessStats(@NotNull DriverProcessStats driverProcessStats)
    {
        synchronized (driverProcessStats) {
            this.numProcessInternalCalls = driverProcessStats.getNumProcessInternalCalls();
            this.numProcessCalls = driverProcessStats.getNumProcessCalls();
            this.totalNumPagesMoved = driverProcessStats.getTotalNumPagesMoved();
            this.totalNumCachedPagesUsed = driverProcessStats.getTotalNumCachedPagesUsed();
            this.totalNumBlockedProcessInternalCount = driverProcessStats.getTotalNumBlockedProcessInternalCount();
            this.totalNumBlockedProcessCount = driverProcessStats.getTotalNumBlockedProcessCount();
            this.firstBlockedReasons = driverProcessStats.getFirstBlockedReasons();
            this.numIterationsInProcess = driverProcessStats.getNumIterationsInProcess();
            this.nanosInProcess = driverProcessStats.getNanosInProcess();
        }
    }

    public DriverProcessStats() {}

    public synchronized long getNumIterationsInProcess()
    {
        return numIterationsInProcess;
    }

    public synchronized long getNanosInProcess()
    {
        return nanosInProcess;
    }

    public synchronized long getNumProcessInternalCalls()
    {
        return numProcessInternalCalls;
    }

    public synchronized long getTotalNumPagesMoved()
    {
        return totalNumPagesMoved;
    }

    public synchronized long getTotalNumCachedPagesUsed()
    {
        return totalNumCachedPagesUsed;
    }

    public synchronized long getTotalNumBlockedProcessInternalCount()
    {
        return totalNumBlockedProcessInternalCount;
    }

    public synchronized Map<OperatorBlockedReason, AtomicLong> getFirstBlockedReasons()
    {
        return ImmutableMap.copyOf(firstBlockedReasons);
    }

    public synchronized long getNumProcessCalls()
    {
        return numProcessCalls;
    }

    public synchronized long getTotalNumBlockedProcessCount()
    {
        return totalNumBlockedProcessCount;
    }

    public synchronized void recordProcessCall(ListenableFuture<?> blockedFuture, int numIterations, long timeInNanos)
    {
        ++numProcessCalls;
        totalNumBlockedProcessCount += blockedFuture.isDone() ? 0L : 1L;
        numIterationsInProcess += numIterations;
        nanosInProcess += timeInNanos;
    }

    public synchronized void recordProcessInternalCall(long numPagesMoved, long numCachedPagesUsed, ListenableFuture<?> blockedFuture, List<OperatorBlockedReason> blockedReasons)
    {
        ++numProcessInternalCalls;
        totalNumPagesMoved += numPagesMoved;
        totalNumCachedPagesUsed += numCachedPagesUsed;
        totalNumBlockedProcessInternalCount += blockedFuture.isDone() ? 0L : 1L;
        if (!blockedReasons.isEmpty()) {
            firstBlockedReasons.compute(blockedReasons.get(0), (reason, cur) -> {
                if (cur == null) {
                    return new AtomicLong(1L);
                }
                else {
                    cur.incrementAndGet();
                    return cur;
                }
            });
        }
    }
}
