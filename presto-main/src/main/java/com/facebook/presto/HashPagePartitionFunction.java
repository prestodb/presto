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

import com.facebook.presto.operator.HashGenerator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.operator.HashGenerator.createHashGenerator;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class HashPagePartitionFunction
        implements PagePartitionFunction
{
    private final int partition;
    private final int partitionCount;
    private final List<Integer> partitioningChannels;
    private final List<Type> types;
    private final HashGenerator hashGenerator;
    private final Optional<Integer> hashChannel;

    @JsonCreator
    public HashPagePartitionFunction(
            @JsonProperty("partition") int partition,
            @JsonProperty("partitionCount") int partitionCount,
            @JsonProperty("partitioningChannels") List<Integer> partitioningChannels,
            @JsonProperty("hashChannel") Optional<Integer> hashChannel,
            @JsonProperty("types") List<Type> types)
    {
        checkNotNull(partitioningChannels, "partitioningChannels is null");
        checkArgument(!partitioningChannels.isEmpty(), "partitioningChannels is empty");
        this.hashChannel = checkNotNull(hashChannel, "hashChannel is null");
        checkArgument(!hashChannel. isPresent() || hashChannel.get() < types.size(), "invalid hashChannel");

        this.partition = partition;
        this.partitionCount = partitionCount;
        this.partitioningChannels = ImmutableList.copyOf(partitioningChannels);
        this.hashGenerator = createHashGenerator(hashChannel, partitioningChannels, types);
        this.types = ImmutableList.copyOf(types);
    }

    @JsonProperty
    public int getPartition()
    {
        return partition;
    }

    @JsonProperty
    public int getPartitionCount()
    {
        return partitionCount;
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
    public List<Page> partition(List<Page> pages)
    {
        if (pages.isEmpty()) {
            return pages;
        }
        PageBuilder pageBuilder = new PageBuilder(types);

        ImmutableList.Builder<Page> partitionedPages = ImmutableList.builder();
        for (Page page : pages) {
            for (int position = 0; position < page.getPositionCount(); position++) {
                // if hash is not in range skip
                int partitionHashBucket = hashGenerator.getPartitionHashBucket(partitionCount, position, page);
                if (partitionHashBucket != partition) {
                    continue;
                }

                pageBuilder.declarePosition();
                for (int channel = 0; channel < types.size(); channel++) {
                    Type type = types.get(channel);
                    type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
                }

                // if page is full, flush
                if (pageBuilder.isFull()) {
                    partitionedPages.add(pageBuilder.build());
                    pageBuilder.reset();
                }
            }
        }
        if (!pageBuilder.isEmpty()) {
            partitionedPages.add(pageBuilder.build());
        }

        return partitionedPages.build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partition, partitionCount, partitioningChannels, hashGenerator);
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
