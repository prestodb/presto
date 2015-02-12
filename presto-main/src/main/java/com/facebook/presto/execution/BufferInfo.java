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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class BufferInfo
{
    private final TaskId bufferId;
    private final boolean finished;
    private final int bufferedPages;
    private final long pagesSent;

    @JsonCreator
    public BufferInfo(
            @JsonProperty("bufferId") TaskId bufferId,
            @JsonProperty("finished") boolean finished,
            @JsonProperty("bufferedPages") int bufferedPages,
            @JsonProperty("pagesSent") long pagesSent)
    {
        Preconditions.checkNotNull(bufferId, "bufferId is null");

        this.bufferId = bufferId;
        this.finished = finished;
        this.bufferedPages = bufferedPages;
        this.pagesSent = pagesSent;
    }

    @JsonProperty
    public TaskId getBufferId()
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

    @JsonProperty
    public long getPagesSent()
    {
        return pagesSent;
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
        final BufferInfo other = (BufferInfo) obj;
        return Objects.equals(this.bufferId, other.bufferId) &&
                Objects.equals(this.finished, other.finished) &&
                Objects.equals(this.bufferedPages, other.bufferedPages) &&
                Objects.equals(this.pagesSent, other.pagesSent);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bufferId, finished, bufferedPages, pagesSent);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bufferId", bufferId)
                .add("finished", finished)
                .add("bufferedPages", bufferedPages)
                .add("pagesSent", pagesSent)
                .toString();
    }
}
