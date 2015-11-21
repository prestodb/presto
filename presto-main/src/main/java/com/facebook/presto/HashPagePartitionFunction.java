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
package com.facebook.presto;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanFragment.NullPartitioning;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class HashPagePartitionFunction
        extends PartitionedPagePartitionFunction
{
    private final List<Integer> partitioningChannels;
    private final List<Type> types;
    private final Optional<Integer> hashChannel;
    private final NullPartitioning nullPartitioning;

    @JsonCreator
    public HashPagePartitionFunction(
            @JsonProperty("partition") int partition,
            @JsonProperty("partitionCount") int partitionCount,
            @JsonProperty("partitioningChannels") List<Integer> partitioningChannels,
            @JsonProperty("hashChannel") Optional<Integer> hashChannel,
            @JsonProperty("types") List<Type> types,
            @JsonProperty("nullPartitioning") NullPartitioning nullPartitioning)
    {
        super(partition, partitionCount);

        requireNonNull(partitioningChannels, "partitioningChannels is null");
        checkArgument(!partitioningChannels.isEmpty(), "partitioningChannels is empty");
        this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
        checkArgument(!hashChannel.isPresent() || hashChannel.get() < types.size(), "invalid hashChannel");
        checkArgument(nullPartitioning == NullPartitioning.HASH || partitioningChannels.size() == 1,
                "size of partitioningChannels is not 1 when nullPartition is REPLICATE.");

        this.partitioningChannels = ImmutableList.copyOf(partitioningChannels);
        this.types = ImmutableList.copyOf(types);
        this.nullPartitioning = nullPartitioning;
    }

    @JsonProperty
    public List<Integer> getPartitioningChannels()
    {
        return partitioningChannels;
    }

    @JsonProperty
    public List<Type> getTypes()
    {
        return types;
    }

    @JsonProperty
    public Optional<Integer> getHashChannel()
    {
        return hashChannel;
    }

    @JsonProperty
    public NullPartitioning getNullPartitioning()
    {
        return nullPartitioning;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partition, partitionCount, partitioningChannels);
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
        HashPagePartitionFunction other = (HashPagePartitionFunction) obj;
        return Objects.equals(this.partition, other.partition) &&
                Objects.equals(this.partitionCount, other.partitionCount) &&
                Objects.equals(this.partitioningChannels, other.partitioningChannels) &&
                Objects.equals(hashChannel, other.hashChannel);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partition", partition)
                .add("partitionCount", partitionCount)
                .add("partitioningChannels", partitioningChannels)
                .add("hashChannel", hashChannel)
                .toString();
    }
}
