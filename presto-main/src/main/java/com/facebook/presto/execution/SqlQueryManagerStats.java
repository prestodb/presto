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
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class SqlQueryManagerStats
{
    private final CounterStat startedQueries = new CounterStat();
    private final CounterStat completedQueries = new CounterStat();
    private final DistributionStat executionTime = new DistributionStat();

    public void queryStarted()
    {
        startedQueries.update(1);
    }

    public void queryFinished(QueryInfo info)
    {
        completedQueries.update(1);
        executionTime.add(info.getQueryStats().getEndTime().getMillis() - info.getQueryStats().getCreateTime().getMillis());
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
    public DistributionStat getExecutionTime()
    {
        return executionTime;
    }
}
