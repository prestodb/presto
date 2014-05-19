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

import com.facebook.presto.spi.StandardErrorCode;
import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class SqlQueryManagerStats
{
    private final CounterStat startedQueries = new CounterStat();
    private final CounterStat completedQueries = new CounterStat();
    private final CounterStat failedQueries = new CounterStat();
    private final CounterStat abandonedQueries = new CounterStat();
    private final CounterStat canceledQueries = new CounterStat();
    private final CounterStat userErrorFailures = new CounterStat();
    private final CounterStat internalFailures = new CounterStat();
    private final CounterStat externalFailures = new CounterStat();
    private final CounterStat insufficientResourcesFailures = new CounterStat();
    private final DistributionStat executionTime = new DistributionStat();

    public void queryStarted()
    {
        startedQueries.update(1);
    }

    public void queryFinished(QueryInfo info)
    {
        completedQueries.update(1);
        executionTime.add(info.getQueryStats().getEndTime().getMillis() - info.getQueryStats().getCreateTime().getMillis());

        if (info.getErrorCode() != null) {
            switch (StandardErrorCode.toErrorType(info.getErrorCode().getCode())) {
                case USER_ERROR:
                    userErrorFailures.update(1);
                    break;
                case INTERNAL:
                    internalFailures.update(1);
                    break;
                case INSUFFICIENT_RESOURCES:
                    insufficientResourcesFailures.update(1);
                    break;
                case EXTERNAL:
                    externalFailures.update(1);
                    break;
            }

            if (info.getErrorCode().getCode() == StandardErrorCode.ABANDONED_QUERY.toErrorCode().getCode()) {
                abandonedQueries.update(1);
            }
            else if (info.getErrorCode().getCode() == StandardErrorCode.USER_CANCELED.toErrorCode().getCode()) {
                canceledQueries.update(1);
            }
            failedQueries.update(1);
        }
    }

    @Managed
    public long getRunningQueries()
    {
        return Math.max(0, startedQueries.getTotalCount() - completedQueries.getTotalCount());
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
    public DistributionStat getExecutionTime()
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
}
