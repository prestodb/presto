/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.execution.PageBuffer.BufferState;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;

public class PageBufferInfo
{
    private final String bufferId;
    private final BufferState state;
    private final int bufferedPages;

    @JsonCreator
    public PageBufferInfo(
            @JsonProperty("bufferId") String bufferId,
            @JsonProperty("state") BufferState state,
            @JsonProperty("bufferedPages") int bufferedPages)
    {
        Preconditions.checkNotNull(bufferId, "bufferId is null");
        Preconditions.checkNotNull(state, "state is null");

        this.bufferId = bufferId;
        this.state = state;
        this.bufferedPages = bufferedPages;
    }

    @JsonProperty
    public String getBufferId()
    {
        return bufferId;
    }

    @JsonProperty
    public BufferState getState()
    {
        return state;
    }

    @JsonProperty
    public int getBufferedPages()
    {
        return bufferedPages;
    }

    public static Function<PageBufferInfo, String> bufferIdGetter()
    {
        return new Function<PageBufferInfo, String>()
        {
            @Override
            public String apply(PageBufferInfo taskInfo)
            {
                return taskInfo.getBufferId();
            }
        };
    }

}
