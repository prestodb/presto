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
package com.facebook.presto.sql.planner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PartitionFunctionBinding
{
    private final PartitionFunctionHandle functionHandle;
    private final List<Integer> partitioningChannels;
    private final Optional<Integer> hashChannel;
    private final boolean replicateNulls;
    private final OptionalInt partitionCount;

    @JsonCreator
    public PartitionFunctionBinding(
            @JsonProperty("functionHandle") PartitionFunctionHandle functionHandle,
            @JsonProperty("partitioningChannels") List<Integer> partitioningChannels,
            @JsonProperty("hashChannel") Optional<Integer> hashChannel,
            @JsonProperty("replicateNulls") boolean replicateNulls,
            @JsonProperty("partitionCount") OptionalInt partitionCount)
    {
        this.functionHandle = requireNonNull(functionHandle, "functionHandle is null");
        this.partitioningChannels = ImmutableList.copyOf(requireNonNull(partitioningChannels, "partitioningChannels is null"));
        this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
        checkArgument(!replicateNulls || partitioningChannels.size() == 1, "size of partitioningChannels is not 1 when nullPartition is REPLICATE.");
        this.replicateNulls = replicateNulls;
        this.partitionCount = requireNonNull(partitionCount, "partitionCount is null");
    }

    @JsonProperty
    public PartitionFunctionHandle getFunctionHandle()
    {
        return functionHandle;
    }

    @JsonProperty
    public List<Integer> getPartitioningChannels()
    {
        return partitioningChannels;
    }

    @JsonProperty
    public Optional<Integer> getHashChannel()
    {
        return hashChannel;
    }

    @JsonProperty
    public boolean isReplicateNulls()
    {
        return replicateNulls;
    }

    @JsonProperty
    public OptionalInt getPartitionCount()
    {
        return partitionCount;
    }

    public PartitionFunctionBinding withPartitionCount(OptionalInt partitionCount)
    {
        return new PartitionFunctionBinding(functionHandle, partitioningChannels, hashChannel, replicateNulls, partitionCount);
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
        PartitionFunctionBinding that = (PartitionFunctionBinding) o;
        return Objects.equals(functionHandle, that.functionHandle) &&
                Objects.equals(partitioningChannels, that.partitioningChannels) &&
                Objects.equals(hashChannel, that.hashChannel) &&
                replicateNulls == that.replicateNulls &&
                Objects.equals(partitionCount, that.partitionCount);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(functionHandle, partitioningChannels, replicateNulls, partitionCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("functionId", functionHandle)
                .add("partitioningChannels", partitioningChannels)
                .add("hashChannel", hashChannel)
                .add("replicateNulls", replicateNulls)
                .add("partitionCount", partitionCount)
                .toString();
    }
}
