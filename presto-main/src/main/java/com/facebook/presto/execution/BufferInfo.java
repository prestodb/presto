/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class BufferInfo
{
    private final String bufferId;
    private final boolean finished;
    private final int bufferedPages;

    @JsonCreator
    public BufferInfo(
            @JsonProperty("bufferId") String bufferId,
            @JsonProperty("finished") boolean finished,
            @JsonProperty("bufferedPages") int bufferedPages)
    {
        Preconditions.checkNotNull(bufferId, "bufferId is null");

        this.bufferId = bufferId;
        this.finished = finished;
        this.bufferedPages = bufferedPages;
    }

    @JsonProperty
    public String getBufferId()
    {
        return bufferId;
    }

    @JsonProperty
    public boolean isFinished()
    {
        return finished;
    }

    @JsonProperty
    public int getBufferedPages()
    {
        return bufferedPages;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BufferInfo that = (BufferInfo) o;

        if (bufferedPages != that.bufferedPages) {
            return false;
        }
        if (finished != that.finished) {
            return false;
        }
        if (!bufferId.equals(that.bufferId)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = bufferId.hashCode();
        result = 31 * result + (finished ? 1 : 0);
        result = 31 * result + bufferedPages;
        return result;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("bufferId", bufferId)
                .add("finished", finished)
                .add("bufferedPages", bufferedPages)
                .toString();
    }

    public static Function<BufferInfo, String> bufferIdGetter()
    {
        return new Function<BufferInfo, String>()
        {
            @Override
            public String apply(BufferInfo taskInfo)
            {
                return taskInfo.getBufferId();
            }
        };
    }

}
