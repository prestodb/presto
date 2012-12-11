/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import io.airlift.units.Duration;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.DateTime;

public class QueryStats
{
    private final DateTime createTime;
    private DateTime startTime;
    private DateTime endTime;
    // todo these should be duration objects
    // times are in ms
    private long analysisTime;
    private long distributedPlanningTime;

    public QueryStats()
    {
        createTime = DateTime.now();
    }

    @JsonCreator
    public QueryStats(
            @JsonProperty("createTime") DateTime createTime,
            @JsonProperty("startTime") DateTime startTime,
            @JsonProperty("endTime") DateTime endTime,
            @JsonProperty("analysisTime") long analysisTime,
            @JsonProperty("distributedPlanningTime") long distributedPlanningTime)
    {
        this.createTime = createTime;
        this.startTime = startTime;
        this.endTime = endTime;
        this.analysisTime = analysisTime;
        this.distributedPlanningTime = distributedPlanningTime;
    }

    @JsonProperty
    public DateTime getCreateTime()
    {
        return createTime;
    }

    @JsonProperty
    public DateTime getStartTime()
    {
        return startTime;
    }

    @JsonProperty
    public DateTime getEndTime()
    {
        return endTime;
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

    public void recordStart()
    {
        this.startTime = DateTime.now();
    }

    public void recordEnd()
    {
        this.endTime = DateTime.now();
    }

    public void recordAnalysisTime(long analysisStart)
    {
        analysisTime = (long) Duration.nanosSince(analysisStart).toMillis();
    }

    public void recordDistributedPlanningTime(long distributedPlanningStart)
    {
        distributedPlanningTime = (long) Duration.nanosSince(distributedPlanningStart).toMillis();
    }
}
