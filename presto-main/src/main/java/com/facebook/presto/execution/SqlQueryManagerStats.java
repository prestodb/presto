package com.facebook.presto.execution;

import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.concurrent.TimeUnit;

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
