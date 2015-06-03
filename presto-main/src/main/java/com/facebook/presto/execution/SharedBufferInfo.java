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

import com.facebook.presto.execution.SharedBuffer.BufferState;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public final class SharedBufferInfo
{
    private final BufferState state;
    private final boolean canAddBuffers;
    private final boolean canAddPages;
    private final long totalBufferedBytes;
    private final long totalBufferedPages;
    private final long totalQueuedPages;
    private final long totalPagesSent;
    private final List<BufferInfo> buffers;

    @JsonCreator
    public SharedBufferInfo(
            @JsonProperty("state") BufferState state,
            @JsonProperty("canAddBuffers") boolean canAddBuffers,
            @JsonProperty("canAddPages") boolean canAddPages,
            @JsonProperty("totalBufferedBytes") long totalBufferedBytes,
            @JsonProperty("totalBufferedPages") long totalBufferedPages,
            @JsonProperty("totalQueuedPages") long totalQueuedPages,
            @JsonProperty("totalPagesSent") long totalPagesSent,
            @JsonProperty("buffers") List<BufferInfo> buffers)
    {
        this.state = state;
        this.canAddBuffers = canAddBuffers;
        this.canAddPages = canAddPages;
        this.totalBufferedBytes = totalBufferedBytes;
        this.totalBufferedPages = totalBufferedPages;
        this.totalQueuedPages = totalQueuedPages;
        this.totalPagesSent = totalPagesSent;
        this.buffers = ImmutableList.copyOf(buffers);
    }

    @JsonProperty
    public BufferState getState()
    {
        return state;
    }

    @JsonProperty
    public List<BufferInfo> getBuffers()
    {
        return buffers;
    }

    @JsonProperty
    public boolean isCanAddBuffers()
    {
        return canAddBuffers;
    }

    @JsonProperty
    public boolean isCanAddPages()
    {
        return canAddPages;
    }

    @JsonProperty
    public long getTotalBufferedBytes()
    {
        return totalBufferedBytes;
    }

    @JsonProperty
    public long getTotalBufferedPages()
    {
        return totalBufferedPages;
    }

    @JsonProperty
    public long getTotalQueuedPages()
    {
        return totalQueuedPages;
    }

    @JsonProperty
    public long getTotalPagesSent()
    {
        return totalPagesSent;
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
        SharedBufferInfo that = (SharedBufferInfo) o;
        return Objects.equals(canAddBuffers, that.canAddBuffers) &&
                Objects.equals(canAddPages, that.canAddPages) &&
                Objects.equals(totalBufferedBytes, that.totalBufferedBytes) &&
                Objects.equals(totalBufferedPages, that.totalBufferedPages) &&
                Objects.equals(totalQueuedPages, that.totalQueuedPages) &&
                Objects.equals(totalPagesSent, that.totalPagesSent) &&
                Objects.equals(state, that.state) &&
                Objects.equals(buffers, that.buffers);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(state, canAddBuffers, canAddPages, totalBufferedBytes, totalBufferedPages, totalQueuedPages, totalPagesSent, buffers);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("state", state)
                .add("canAddBuffers", canAddBuffers)
                .add("canAddPages", canAddPages)
                .add("totalBufferedBytes", totalBufferedBytes)
                .add("totalBufferedPages", totalBufferedPages)
                .add("totalQueuedPages", totalQueuedPages)
                .add("totalPagesSent", totalPagesSent)
                .add("buffers", buffers)
                .toString();
    }
}
