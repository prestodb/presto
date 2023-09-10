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

import com.facebook.airlift.stats.CounterStat;
import com.facebook.airlift.stats.DistributionStat;
import com.facebook.airlift.stats.TimeStat;
import com.facebook.presto.operator.OperatorBlockedReason;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MergedDriverProcessStats
{
    private static final AtomicLong ZERO = new AtomicLong(0L);
    @GuardedBy("this")
    private final CounterStat numProcessCalls = new CounterStat();
    @GuardedBy("this")
    private final CounterStat numProcessInternalCalls = new CounterStat();
    @GuardedBy("this")
    private final DistributionStat numPagesMoved = new DistributionStat();
    @GuardedBy("this")
    private final DistributionStat numCachedPagesMoved = new DistributionStat();
    @GuardedBy("this")
    private final CounterStat numBlockedProcessCalls = new CounterStat();
    @GuardedBy("this")
    private final Map<OperatorBlockedReason, CounterStat> firstBlockedReasons = new HashMap<>();

    @GuardedBy("this")
    private final TimeStat timeInProcess = new TimeStat(TimeUnit.NANOSECONDS);
    @GuardedBy("this")
    private final CounterStat numIterationsInProcess = new CounterStat();

    public synchronized void record(Optional<DriverProcessStats> beforeProcess, Optional<DriverProcessStats> afterProcess)
    {
        if (!beforeProcess.isPresent() || !afterProcess.isPresent()) {
            return;
        }
        DriverProcessStats before = beforeProcess.get();
        DriverProcessStats after = afterProcess.get();
        numProcessInternalCalls.update(after.getNumProcessInternalCalls() - before.getNumProcessInternalCalls());
        numProcessCalls.update(after.getNumProcessCalls() - before.getNumProcessCalls());
        numPagesMoved.add(after.getTotalNumPagesMoved() - before.getTotalNumPagesMoved());
        numCachedPagesMoved.add(after.getTotalNumCachedPagesUsed() - before.getTotalNumCachedPagesUsed());
        numBlockedProcessCalls.update(after.getTotalNumBlockedProcessInternalCount() - before.getTotalNumBlockedProcessInternalCount());
        timeInProcess.add(after.getNanosInProcess() - before.getNanosInProcess(), TimeUnit.NANOSECONDS);
        numIterationsInProcess.update(after.getNumIterationsInProcess() - before.getNumIterationsInProcess());

        // In one iteration of the driver only one (first) blocked reason should have been increased.
        for (Map.Entry<OperatorBlockedReason, AtomicLong> entry : after.getFirstBlockedReasons().entrySet()) {
            OperatorBlockedReason reason = entry.getKey();
            AtomicLong count = entry.getValue();
            long increase = count.get() - before.getFirstBlockedReasons().getOrDefault(reason, ZERO).get();
            if (increase > 0) {
                firstBlockedReasons.compute(reason, (oldReason, oldCount) -> {
                    CounterStat newCounter = oldCount != null ? oldCount : new CounterStat();
                    newCounter.update(increase);
                    return newCounter;
                });
                break;
            }
        }
    }

    @Managed
    @Nested
    public synchronized CounterStat getNumProcessCalls()
    {
        return numProcessCalls;
    }

    @Managed
    @Nested
    public synchronized CounterStat getNumProcessInternalCalls()
    {
        return numProcessInternalCalls;
    }

    @Managed
    @Nested
    public synchronized DistributionStat getNumPagesMoved()
    {
        return numPagesMoved;
    }

    @Managed
    @Nested
    public synchronized DistributionStat getNumCachedPagesMoved()
    {
        return numCachedPagesMoved;
    }

    @Managed
    @Nested
    public synchronized CounterStat getNumBlockedProcessCalls()
    {
        return numBlockedProcessCalls;
    }

    @Managed
    @Nested
    public synchronized CounterStat getNumRevokingBlockedReason()
    {
        return firstBlockedReasons.computeIfAbsent(OperatorBlockedReason.REVOKING, ignored -> new CounterStat());
    }

    @Managed
    @Nested
    public synchronized CounterStat getNumIsBlockedBlockedReason()
    {
        return firstBlockedReasons.computeIfAbsent(OperatorBlockedReason.IS_BLOCKED, ignored -> new CounterStat());
    }

    @Managed
    @Nested
    public synchronized CounterStat getNumWaitingForMemoryBlockedReason()
    {
        return firstBlockedReasons.computeIfAbsent(OperatorBlockedReason.WAITING_FOR_MEMORY, ignored -> new CounterStat());
    }

    @Managed
    @Nested
    public synchronized CounterStat getNumWaitingForRevocableMemoryBlockedReason()
    {
        return firstBlockedReasons.computeIfAbsent(OperatorBlockedReason.WAITING_FOR_REVOCABLE_MEMORY, ignored -> new CounterStat());
    }

    @Managed
    @Nested
    public TimeStat getTimeInProcess()
    {
        return timeInProcess;
    }

    @Managed
    @Nested
    public CounterStat getNumIterationsInProcess()
    {
        return numIterationsInProcess;
    }
}
