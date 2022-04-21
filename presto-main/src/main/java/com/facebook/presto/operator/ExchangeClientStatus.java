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
package com.facebook.presto.operator;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.util.Mergeable;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public class ExchangeClientStatus
        implements Mergeable<ExchangeClientStatus>, OperatorInfo
{
    private final long bufferedBytes;
    private final long maxBufferedBytes;
    private final long averageBytesPerRequest;
    private final long successfulRequestsCount;
    private final int bufferedPages;
    private final boolean noMoreLocations;
    private final List<PageBufferClientStatus> pageBufferClientStatuses;

    @JsonCreator
    @ThriftConstructor
    public ExchangeClientStatus(
            @JsonProperty("bufferedBytes") long bufferedBytes,
            @JsonProperty("maxBufferedBytes") long maxBufferedBytes,
            @JsonProperty("averageBytesPerRequest") long averageBytesPerRequest,
            @JsonProperty("successfulRequestsCount") long successfulRequestsCount,
            @JsonProperty("bufferedPages") int bufferedPages,
            @JsonProperty("noMoreLocations") boolean noMoreLocations,
            @JsonProperty("pageBufferClientStatuses") List<PageBufferClientStatus> pageBufferClientStatuses)
    {
        this.bufferedBytes = bufferedBytes;
        this.maxBufferedBytes = maxBufferedBytes;
        this.averageBytesPerRequest = averageBytesPerRequest;
        this.successfulRequestsCount = successfulRequestsCount;
        this.bufferedPages = bufferedPages;
        this.noMoreLocations = noMoreLocations;
        this.pageBufferClientStatuses = ImmutableList.copyOf(requireNonNull(pageBufferClientStatuses, "pageBufferClientStatuses is null"));
    }

    @JsonProperty
    @ThriftField(1)
    public long getBufferedBytes()
    {
        return bufferedBytes;
    }

    @JsonProperty
    @ThriftField(2)
    public long getMaxBufferedBytes()
    {
        return maxBufferedBytes;
    }

    @JsonProperty
    @ThriftField(3)
    public long getAverageBytesPerRequest()
    {
        return averageBytesPerRequest;
    }

    @JsonProperty
    @ThriftField(4)
    public long getSuccessfulRequestsCount()
    {
        return successfulRequestsCount;
    }

    @JsonProperty
    @ThriftField(5)
    public int getBufferedPages()
    {
        return bufferedPages;
    }

    @JsonProperty
    @ThriftField(6)
    public boolean isNoMoreLocations()
    {
        return noMoreLocations;
    }

    @JsonProperty
    @ThriftField(7)
    public List<PageBufferClientStatus> getPageBufferClientStatuses()
    {
        return pageBufferClientStatuses;
    }

    @Override
    public boolean isFinal()
    {
        return true;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bufferBytes", bufferedBytes)
                .add("maxBufferedBytes", maxBufferedBytes)
                .add("averageBytesPerRequest", averageBytesPerRequest)
                .add("successfulRequestsCount", successfulRequestsCount)
                .add("bufferedPages", bufferedPages)
                .add("noMoreLocations", noMoreLocations)
                .add("pageBufferClientStatuses", pageBufferClientStatuses)
                .toString();
    }

    @Override
    public ExchangeClientStatus mergeWith(ExchangeClientStatus other)
    {
        return new ExchangeClientStatus(
                (bufferedBytes + other.bufferedBytes) / 2, // this is correct as long as all clients have the same buffer size (capacity)
                Math.max(maxBufferedBytes, other.maxBufferedBytes),
                mergeAvgs(averageBytesPerRequest, successfulRequestsCount, other.averageBytesPerRequest, other.successfulRequestsCount),
                successfulRequestsCount + other.successfulRequestsCount,
                bufferedPages + other.bufferedPages,
                noMoreLocations && other.noMoreLocations, // if at least one has some locations, merge has some too
                ImmutableList.of()); // pageBufferClientStatuses may be long, so we don't want to combine the lists
    }

    private static long mergeAvgs(long value1, long count1, long value2, long count2)
    {
        if (count1 == 0) {
            return value2;
        }
        if (count2 == 0) {
            return value1;
        }
        // AVG_n+m = AVG_n * n / (n + m) + AVG_m * m / (n + m)
        return (value1 * count1 / (count1 + count2)) + (value2 * count2 / (count1 + count2));
    }
}
