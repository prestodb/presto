/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.google.common.base.Preconditions;
import io.airlift.units.Duration;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.DateTime;

import java.util.concurrent.atomic.AtomicInteger;

public class QueryStats
{
    private final DateTime createTime;
    private final long createNanos;
    private DateTime executionStartTime;
    private DateTime lastHeartBeat;
    private DateTime endTime;
    // todo these should be duration objects
    // times are in ms
    private long queuedTime;
    private long analysisTime;
    private long distributedPlanningTime;

    private AtomicInteger splits = new AtomicInteger();

    public QueryStats()
    {
        createTime = DateTime.now();
        lastHeartBeat = DateTime.now();
        createNanos = System.nanoTime();
    }

    @JsonCreator
    public QueryStats(
            @JsonProperty("createTime") DateTime createTime,
            @JsonProperty("executionStartTime") DateTime executionStartTime,
            @JsonProperty("lastHeartBeat") DateTime lastHeartBeat,
            @JsonProperty("endTime") DateTime endTime,
            @JsonProperty("queuedTime") long queuedTime,
            @JsonProperty("analysisTime") long analysisTime,
            @JsonProperty("distributedPlanningTime") long distributedPlanningTime,
            @JsonProperty("splits") int splits)
    {
        this.createTime = createTime;
        this.executionStartTime = executionStartTime;
        this.endTime = endTime;
        this.queuedTime = queuedTime;
        this.analysisTime = analysisTime;
        this.distributedPlanningTime = distributedPlanningTime;
        this.splits.set(splits);

        createNanos = -1;
    }

    @JsonProperty
    public DateTime getCreateTime()
    {
        return createTime;
    }

    @JsonProperty
    public DateTime getExecutionStartTime()
    {
        return executionStartTime;
    }

    @JsonProperty
    public DateTime getLastHeartBeat()
    {
        return lastHeartBeat;
    }

    @JsonProperty
    public DateTime getEndTime()
    {
        return endTime;
    }

    @JsonProperty
    public long getQueuedTime()
    {
        return queuedTime;
    }

    @JsonProperty
    public long getAnalysisTime()
    {
        return analysisTime;
    }

    @JsonProperty
    public long getDistributedPlanningTime()
    {
        return distributedPlanningTime;
    }

    @JsonProperty
    public int getSplits()
    {
        return splits.get();
    }

    public void recordAnalysisStart()
    {
        Preconditions.checkState(createNanos > 0, "Can not record analysis start");
        queuedTime = (long) Duration.nanosSince(createNanos).toMillis();
    }

    public void recordHeartBeat()
    {
        this.lastHeartBeat = DateTime.now();
    }

    public void recordExecutionStart()
    {
        this.executionStartTime = DateTime.now();
    }

    public void recordEnd()
    {
        if (endTime == null) {
            endTime = DateTime.now();
        }
    }

    public void recordAnalysisTime(long analysisStart)
    {
        analysisTime = (long) Duration.nanosSince(analysisStart).toMillis();
    }

    public void recordDistributedPlanningTime(long distributedPlanningStart)
    {
        distributedPlanningTime = (long) Duration.nanosSince(distributedPlanningStart).toMillis();
    }

    public void addSplits(int splits)
    {
        this.splits.addAndGet(splits);
    }
}
