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
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class BufferInfo
{
    private final String bufferId;
    private final boolean finished;
    private final int bufferedPages;
    private final long pagesSent;

    @JsonCreator
    public BufferInfo(
            @JsonProperty("bufferId") String bufferId,
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
        return Objects.equal(this.bufferId, other.bufferId) &&
                Objects.equal(this.finished, other.finished) &&
                Objects.equal(this.bufferedPages, other.bufferedPages) &&
                Objects.equal(this.pagesSent, other.pagesSent);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(bufferId, finished, bufferedPages, pagesSent);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("bufferId", bufferId)
                .add("finished", finished)
                .add("bufferedPages", bufferedPages)
                .add("pagesSent", pagesSent)
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
