/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.split.Split;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class ScheduledSplit
{
    private final long sequenceId;
    private final Map<PlanNodeId, ? extends Split> splits;

    @JsonCreator
    public ScheduledSplit(@JsonProperty("sequenceId") long sequenceId,
            @JsonProperty("splits") Map<PlanNodeId, ? extends Split> splits)
    {
        this.sequenceId = sequenceId;
        this.splits = checkNotNull(splits, "splits is null");
    }

    @JsonProperty
    public long getSequenceId()
    {
        return sequenceId;
    }

    @JsonProperty
    public Map<PlanNodeId, ? extends Split> getSplits()
    {
        return splits;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("sequenceId", sequenceId)
                .add("splits", splits)
                .toString();
    }
}
