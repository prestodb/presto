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
package com.facebook.presto.execution.buffer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public final class OutputBufferInfo
{
    private final String type;
    private final BufferState state;
    private final boolean canAddBuffers;
    private final boolean canAddPages;
    private final long totalBufferedBytes;
    private final long totalBufferedPages;
    private final long totalRowsSent;
    private final long totalPagesSent;
    private final List<BufferInfo> buffers;

    @JsonCreator
    public OutputBufferInfo(
            @JsonProperty("type") String type,
            @JsonProperty("state") BufferState state,
            @JsonProperty("canAddBuffers") boolean canAddBuffers,
            @JsonProperty("canAddPages") boolean canAddPages,
            @JsonProperty("totalBufferedBytes") long totalBufferedBytes,
            @JsonProperty("totalBufferedPages") long totalBufferedPages,
            @JsonProperty("totalRowsSent") long totalRowsSent,
            @JsonProperty("totalPagesSent") long totalPagesSent,
            @JsonProperty("buffers") List<BufferInfo> buffers)
    {
        this.type = type;
        this.state = state;
        this.canAddBuffers = canAddBuffers;
        this.canAddPages = canAddPages;
        this.totalBufferedBytes = totalBufferedBytes;
        this.totalBufferedPages = totalBufferedPages;
        this.totalRowsSent = totalRowsSent;
        this.totalPagesSent = totalPagesSent;
        this.buffers = ImmutableList.copyOf(buffers);
    }

    @JsonProperty
    public String getType()
    {
        return type;
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
    public long getTotalRowsSent()
    {
        return totalRowsSent;
    }

    @JsonProperty
    public long getTotalPagesSent()
    {
        return totalPagesSent;
    }

    public OutputBufferInfo summarize()
    {
        return new OutputBufferInfo(type, state, canAddBuffers, canAddPages, totalBufferedBytes, totalBufferedPages, totalRowsSent, totalPagesSent, ImmutableList.of());
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
        OutputBufferInfo that = (OutputBufferInfo) o;
        return Objects.equals(type, that.type) &&
                Objects.equals(canAddBuffers, that.canAddBuffers) &&
                Objects.equals(canAddPages, that.canAddPages) &&
                Objects.equals(totalBufferedBytes, that.totalBufferedBytes) &&
                Objects.equals(totalBufferedPages, that.totalBufferedPages) &&
                Objects.equals(totalRowsSent, that.totalRowsSent) &&
                Objects.equals(totalPagesSent, that.totalPagesSent) &&
                Objects.equals(state, that.state) &&
                Objects.equals(buffers, that.buffers);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(state, canAddBuffers, canAddPages, totalBufferedBytes, totalBufferedPages, totalRowsSent, totalPagesSent, buffers);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .add("state", state)
                .add("canAddBuffers", canAddBuffers)
                .add("canAddPages", canAddPages)
                .add("totalBufferedBytes", totalBufferedBytes)
                .add("totalBufferedPages", totalBufferedPages)
                .add("totalRowsSent", totalRowsSent)
                .add("totalPagesSent", totalPagesSent)
                .add("buffers", buffers)
                .toString();
    }
}
