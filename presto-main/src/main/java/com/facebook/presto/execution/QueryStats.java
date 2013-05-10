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

@ThreadSafe
public class QueryStats
{
    private final DateTime createTime;
    private final long createNanos;
    @GuardedBy("this")
    private DateTime executionStartTime;
    @GuardedBy("this")
    private DateTime lastHeartbeat;
    @GuardedBy("this")
    private DateTime endTime;

    @GuardedBy("this")
    private Duration queuedTime;
    @GuardedBy("this")
    private Duration analysisTime;
    @GuardedBy("this")
    private Duration distributedPlanningTime;

    public QueryStats()
    {
        createTime = DateTime.now();
        lastHeartbeat = DateTime.now();
        createNanos = System.nanoTime();
    }

    @JsonCreator
    public QueryStats(
            @JsonProperty("createTime") DateTime createTime,
            @JsonProperty("executionStartTime") DateTime executionStartTime,
            @JsonProperty("lastHeartbeat") DateTime lastHeartbeat,
            @JsonProperty("endTime") DateTime endTime,
            @JsonProperty("queuedTime") Duration queuedTime,
            @JsonProperty("analysisTime") Duration analysisTime,
            @JsonProperty("distributedPlanningTime") Duration distributedPlanningTime)
    {
        this.createTime = createTime;
        this.executionStartTime = executionStartTime;
        this.lastHeartbeat = lastHeartbeat;
        this.endTime = endTime;
        this.queuedTime = queuedTime;
        this.analysisTime = analysisTime;
        this.distributedPlanningTime = distributedPlanningTime;

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
    public synchronized DateTime getLastHeartbeat()
    {
        return lastHeartbeat;
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

    public synchronized void recordAnalysisStart()
    {
        Preconditions.checkState(createNanos > 0, "Can not record analysis start");
        queuedTime = Duration.nanosSince(createNanos);
    }

    public synchronized void recordHeartbeat()
    {
        this.lastHeartbeat = DateTime.now();
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
}
