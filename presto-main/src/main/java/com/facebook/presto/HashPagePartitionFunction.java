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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class HashPagePartitionFunction
        extends PartitionedPagePartitionFunction
{
    private final List<Integer> partitioningChannels;
    private final List<Type> types;
    private final Optional<Integer> hashChannel;

    @JsonCreator
    public HashPagePartitionFunction(
            @JsonProperty("partition") int partition,
            @JsonProperty("partitionCount") int partitionCount,
            @JsonProperty("partitioningChannels") List<Integer> partitioningChannels,
            @JsonProperty("hashChannel") Optional<Integer> hashChannel,
            @JsonProperty("types") List<Type> types)
    {
        super(partition, partitionCount);

        checkNotNull(partitioningChannels, "partitioningChannels is null");
        checkArgument(!partitioningChannels.isEmpty(), "partitioningChannels is empty");
        this.hashChannel = checkNotNull(hashChannel, "hashChannel is null");
        checkArgument(!hashChannel.isPresent() || hashChannel.get() < types.size(), "invalid hashChannel");

        this.partitioningChannels = ImmutableList.copyOf(partitioningChannels);
        this.types = ImmutableList.copyOf(types);
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
