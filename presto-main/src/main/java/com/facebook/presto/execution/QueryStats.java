/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicInteger;

@ThreadSafe
public class QueryStats
{
    private final DateTime createTime;
    private final long createNanos;
    @GuardedBy("this")
    private DateTime executionStartTime;
    @GuardedBy("this")
    private DateTime lastHeartBeat;
    @GuardedBy("this")
    private DateTime endTime;

    @GuardedBy("this")
    private Duration queuedTime;
    @GuardedBy("this")
    private Duration analysisTime;
    @GuardedBy("this")
    private Duration distributedPlanningTime;

    private final AtomicInteger splits = new AtomicInteger();

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
            @JsonProperty("queuedTime") Duration queuedTime,
            @JsonProperty("analysisTime") Duration analysisTime,
            @JsonProperty("distributedPlanningTime") Duration distributedPlanningTime,
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
    public synchronized DateTime getExecutionStartTime()
    {
        return executionStartTime;
    }

    @JsonProperty
    public synchronized DateTime getLastHeartBeat()
    {
        return lastHeartBeat;
    }

    @JsonProperty
    public synchronized DateTime getEndTime()
    {
        return endTime;
    }

    @JsonProperty
    public synchronized Duration getQueuedTime()
    {
        return queuedTime;
    }

    @JsonProperty
    public synchronized Duration getAnalysisTime()
    {
        return analysisTime;
    }

    @JsonProperty
    public synchronized Duration getDistributedPlanningTime()
    {
        return distributedPlanningTime;
    }

    @JsonProperty
    public int getSplits()
    {
        return splits.get();
    }

    public synchronized void recordAnalysisStart()
    {
        Preconditions.checkState(createNanos > 0, "Can not record analysis start");
        queuedTime = Duration.nanosSince(createNanos);
    }

    public synchronized void recordHeartBeat()
    {
        this.lastHeartBeat = DateTime.now();
    }

    public synchronized void recordExecutionStart()
    {
        if (executionStartTime == null) {
            this.executionStartTime = DateTime.now();
        }
    }

    public synchronized void recordEnd()
    {
        if (endTime == null) {
            endTime = DateTime.now();
        }
    }

    public synchronized void recordAnalysisTime(long analysisStart)
    {
        analysisTime = Duration.nanosSince(analysisStart);
    }

    public synchronized void recordDistributedPlanningTime(long distributedPlanningStart)
    {
        distributedPlanningTime = Duration.nanosSince(distributedPlanningStart);
    }

    public void addSplits(int splits)
    {
        this.splits.addAndGet(splits);
    }
}
