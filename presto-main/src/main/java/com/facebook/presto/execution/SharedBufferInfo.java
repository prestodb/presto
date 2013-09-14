/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    private final long masterSequenceId;
    private final long pagesAdded;
    private final List<BufferInfo> buffers;

    @JsonCreator
    public SharedBufferInfo(
            @JsonProperty("state") QueueState state,
            @JsonProperty("masterSequenceId") long masterSequenceId,
            @JsonProperty("pagesAdded") long pagesAdded,
            @JsonProperty("buffers") List<BufferInfo> buffers)
    {
        this.state = state;
        this.masterSequenceId = masterSequenceId;
        this.pagesAdded = pagesAdded;
        this.buffers = ImmutableList.copyOf(buffers);
    }

    @JsonProperty
    public QueueState getState()
    {
        return state;
    }

    @JsonProperty
    public long getMasterSequenceId()
    {
        return masterSequenceId;
    }

    @JsonProperty
    public long getPagesAdded()
    {
        return pagesAdded;
    }

    @JsonProperty
    public List<BufferInfo> getBuffers()
    {
        return buffers;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(state, pagesAdded, buffers, masterSequenceId);
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
                Objects.equal(this.pagesAdded, other.pagesAdded) &&
                Objects.equal(this.buffers, other.buffers) &&
                Objects.equal(this.masterSequenceId, other.masterSequenceId);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("state", state)
                .add("pagesAdded", pagesAdded)
                .add("buffers", buffers)
                .add("masterSequenceId", masterSequenceId)
                .toString();
    }
}
