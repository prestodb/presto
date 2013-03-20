/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class TaskSource
{
    private final PlanNodeId planNodeId;
    private final Set<ScheduledSplit> splits;
    private final boolean noMoreSplits;

    @JsonCreator
    public TaskSource(
            @JsonProperty("planNodeId") PlanNodeId planNodeId,
            @JsonProperty("splits") Set<ScheduledSplit> splits,
            @JsonProperty("noMoreSplits") boolean noMoreSplits)
    {
        this.planNodeId = planNodeId;
        this.splits = ImmutableSet.copyOf(splits);
        this.noMoreSplits = noMoreSplits;
    }

    @JsonProperty
    public PlanNodeId getPlanNodeId()
    {
        return planNodeId;
    }

    @JsonProperty
    public Set<ScheduledSplit> getSplits()
    {
        return splits;
    }

    @JsonProperty
    public boolean isNoMoreSplits()
    {
        return noMoreSplits;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("planNodeId", planNodeId)
                .add("splits", splits)
                .add("noMoreSplits", noMoreSplits)
                .toString();
    }
}
