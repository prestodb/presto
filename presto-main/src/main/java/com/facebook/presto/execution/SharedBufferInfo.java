/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.execution.SharedBuffer.QueueState;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class SharedBufferInfo
{
    private final QueueState state;
    private final List<BufferInfo> buffers;

    @JsonCreator
    public SharedBufferInfo(@JsonProperty("state") QueueState state, @JsonProperty("buffers") List<BufferInfo> buffers)
    {
        this.state = state;
        this.buffers = ImmutableList.copyOf(buffers);
    }

    @JsonProperty
    public QueueState getState()
    {
        return state;
    }

    @JsonProperty
    public List<BufferInfo> getBuffers()
    {
        return buffers;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(state, buffers);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final SharedBufferInfo other = (SharedBufferInfo) obj;
        return Objects.equal(this.state, other.state) &&
                Objects.equal(this.buffers, other.buffers);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("state", state)
                .add("buffers", buffers)
                .toString();
    }
}
