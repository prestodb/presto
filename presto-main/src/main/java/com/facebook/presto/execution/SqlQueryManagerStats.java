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

import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import io.airlift.stats.TimeStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.spi.StandardErrorCode.ABANDONED_QUERY;
import static com.facebook.presto.spi.StandardErrorCode.USER_CANCELED;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SqlQueryManagerStats
{
    private final AtomicInteger runningQueries = new AtomicInteger();
    private final CounterStat startedQueries = new CounterStat();
    private final CounterStat completedQueries = new CounterStat();
    private final CounterStat failedQueries = new CounterStat();
    private final CounterStat abandonedQueries = new CounterStat();
    private final CounterStat canceledQueries = new CounterStat();
    private final CounterStat userErrorFailures = new CounterStat();
    private final CounterStat internalFailures = new CounterStat();
    private final CounterStat externalFailures = new CounterStat();
    private final CounterStat insufficientResourcesFailures = new CounterStat();
    private final TimeStat executionTime = new TimeStat(MILLISECONDS);
    private final DistributionStat wallInputBytesRate = new DistributionStat();
    private final DistributionStat cpuInputByteRate = new DistributionStat();

    public void queryStarted()
    {
        startedQueries.update(1);
        runningQueries.incrementAndGet();
    }

    public void queryStopped()
    {
        runningQueries.decrementAndGet();
    }

    public void queryFinished(QueryInfo info)
    {
        completedQueries.update(1);

        long rawInputBytes = info.getQueryStats().getRawInputDataSize().toBytes();

        long executionWallMillis = info.getQueryStats().getEndTime().getMillis() - info.getQueryStats().getCreateTime().getMillis();
        executionTime.add(executionWallMillis, MILLISECONDS);
        if (executionWallMillis > 0) {
            wallInputBytesRate.add(rawInputBytes * 1000 / executionWallMillis);
        }

        long executionCpuMillis = info.getQueryStats().getTotalCpuTime().toMillis();
        if (executionCpuMillis > 0) {
            cpuInputByteRate.add(rawInputBytes * 1000 / executionCpuMillis);
        }

        if (info.getErrorCode() != null) {
            switch (info.getErrorCode().getType()) {
                case USER_ERROR:
                    userErrorFailures.update(1);
                    break;
                case INTERNAL_ERROR:
                    internalFailures.update(1);
                    break;
                case INSUFFICIENT_RESOURCES:
                    insufficientResourcesFailures.update(1);
                    break;
                case EXTERNAL:
                    externalFailures.update(1);
                    break;
            }

            if (info.getErrorCode().getCode() == ABANDONED_QUERY.toErrorCode().getCode()) {
                abandonedQueries.update(1);
            }
            else if (info.getErrorCode().getCode() == USER_CANCELED.toErrorCode().getCode()) {
                canceledQueries.update(1);
            }
            failedQueries.update(1);
        }
    }

    @Managed
    public long getRunningQueries()
    {
        // This is not startedQueries - completeQueries, since queries can finish without ever starting (cancelled before started, for example)
        return runningQueries.get();
    }

    @Managed
    @Nested
    public CounterStat getStartedQueries()
    {
        return startedQueries;
    }

    @Managed
    @Nested
    public CounterStat getCompletedQueries()
    {
        return completedQueries;
    }

    @Managed
    @Nested
    public CounterStat getFailedQueries()
    {
        return failedQueries;
    }

    @Managed
    @Nested
    public TimeStat getExecutionTime()
    {
        return executionTime;
    }

    @Managed
    @Nested
    public CounterStat getUserErrorFailures()
    {
        return userErrorFailures;
    }

    @Managed
    @Nested
    public CounterStat getInternalFailures()
    {
        return internalFailures;
    }

    @Managed
    @Nested
    public CounterStat getAbandonedQueries()
    {
        return abandonedQueries;
    }

    @Managed
    @Nested
    public CounterStat getCanceledQueries()
    {
        return canceledQueries;
    }

    @Managed
    @Nested
    public CounterStat getExternalFailures()
    {
        return externalFailures;
    }

    @Managed
    @Nested
    public CounterStat getInsufficientResourcesFailures()
    {
        return insufficientResourcesFailures;
    }

    @Managed(description = "Distribution of query input data rates (wall)")
    @Nested
    public DistributionStat getWallInputBytesRate()
    {
        return wallInputBytesRate;
    }

    @Managed(description = "Distribution of query input data rates (cpu)")
    @Nested
    public DistributionStat getCpuInputByteRate()
    {
        return cpuInputByteRate;
    }
}
