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

import static com.google.common.base.Preconditions.checkNotNull;

public class TaskSource
{
    private final Set<PlanNodeId> planNodeIds;
    private final Set<ScheduledSplit> splits;
    private final boolean noMoreSplits;

    @JsonCreator
    public TaskSource(
            @JsonProperty("planNodeIds") Set<PlanNodeId> planNodeIds,
            @JsonProperty("splits") Set<ScheduledSplit> splits,
            @JsonProperty("noMoreSplits") boolean noMoreSplits)
    {
        this.planNodeIds = ImmutableSet.copyOf(checkNotNull(planNodeIds, "nodes is null"));
        this.splits = ImmutableSet.copyOf(checkNotNull(splits, "splits is null"));
        this.noMoreSplits = noMoreSplits;
    }

    @JsonProperty
    public Set<PlanNodeId> getPlanNodeIds()
    {
        return planNodeIds;
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
                .add("planNodeIds", planNodeIds)
                .add("splits", splits)
                .add("noMoreSplits", noMoreSplits)
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(planNodeIds, splits, noMoreSplits);
    }

    @Override
    public boolean equals(Object object)
    {
        if (object == this) {
            return true;
        }
        else if (object == null || (getClass() != object.getClass())) {
            return false;
        }

        TaskSource that = (TaskSource) object;
        return Objects.equal(this.planNodeIds, that.planNodeIds) &&
                Objects.equal(this.splits, that.splits) &&
                this.noMoreSplits == that.noMoreSplits;
    }
}
