/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class OutputBuffers
{
    private final Set<String> bufferIds;
    private final boolean noMoreBufferIds;

    @JsonCreator
    public OutputBuffers(
            @JsonProperty("bufferIds") Set<String> bufferIds,
            @JsonProperty("noMoreBufferIds") boolean noMoreBufferIds)
    {
        this.bufferIds = ImmutableSet.copyOf(bufferIds);
        this.noMoreBufferIds = noMoreBufferIds;
    }

    @JsonProperty
    public Set<String> getBufferIds()
    {
        return bufferIds;
    }

    @JsonProperty
    public boolean isNoMoreBufferIds()
    {
        return noMoreBufferIds;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("bufferIds", bufferIds)
                .add("noMoreBufferIds", noMoreBufferIds)
                .toString();
    }
}
