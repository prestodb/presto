/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.facebook.presto.split.Split;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class ScheduledSplit
{
    private final long sequenceId;
    private final Split split;

    @JsonCreator
    public ScheduledSplit(@JsonProperty("sequenceId") long sequenceId, @JsonProperty("split") Split split)
    {
        this.sequenceId = sequenceId;
        this.split = checkNotNull(split, "split is null");
    }

    @JsonProperty
    public long getSequenceId()
    {
        return sequenceId;
    }

    @JsonProperty
    public Split getSplit()
    {
        return split;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("sequenceId", sequenceId)
                .add("split", split)
                .toString();
    }
}
