/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.google.common.base.Preconditions;
import io.airlift.units.Duration;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
    // todo these should be duration objects
    // times are in ms
    private AtomicLong queuedTime = new AtomicLong();
    private AtomicLong analysisTime = new AtomicLong();
    private AtomicLong distributedPlanningTime = new AtomicLong();

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
        this.queuedTime.set(queuedTime);
        this.analysisTime.set(analysisTime);
        this.distributedPlanningTime.set(distributedPlanningTime);
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
    public long getQueuedTime()
    {
        return queuedTime.get();
    }

    @JsonProperty
    public long getAnalysisTime()
    {
        return analysisTime.get();
    }

    @JsonProperty
    public long getDistributedPlanningTime()
    {
        return distributedPlanningTime.get();
    }

    @JsonProperty
    public int getSplits()
    {
        return splits.get();
    }

    public void recordAnalysisStart()
    {
        Preconditions.checkState(createNanos > 0, "Can not record analysis start");
        queuedTime.set((long) Duration.nanosSince(createNanos).toMillis());
    }

    public synchronized void recordHeartBeat()
    {
        this.lastHeartBeat = DateTime.now();
    }

    public synchronized void recordExecutionStart()
    {
        this.executionStartTime = DateTime.now();
    }

    public synchronized void recordEnd()
    {
        if (endTime == null) {
            endTime = DateTime.now();
        }
    }

    public void recordAnalysisTime(long analysisStart)
    {
        analysisTime.set((long) Duration.nanosSince(analysisStart).toMillis());
    }

    public void recordDistributedPlanningTime(long distributedPlanningStart)
    {
        distributedPlanningTime.set((long) Duration.nanosSince(distributedPlanningStart).toMillis());
    }

    public void addSplits(int splits)
    {
        this.splits.addAndGet(splits);
    }
}
