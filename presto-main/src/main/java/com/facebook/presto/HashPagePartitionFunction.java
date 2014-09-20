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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.type.TypeUtils.hashPosition;
import static com.google.common.base.Preconditions.checkState;

public final class HashPagePartitionFunction
        implements PagePartitionFunction
{
    private final int partition;
    private final int partitionCount;
    private final List<Integer> partitioningChannels;
    private final List<Type> types;

    @JsonCreator
    public HashPagePartitionFunction(
            @JsonProperty("partition") int partition,
            @JsonProperty("partitionCount") int partitionCount,
            @JsonProperty("partitioningChannels") List<Integer> partitioningChannels,
            @JsonProperty("types") List<Type> types)
    {
        this.partition = partition;
        this.partitionCount = partitionCount;
        this.partitioningChannels = ImmutableList.copyOf(partitioningChannels);
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
                int partitionHashBucket = getPartitionHashBucket(position, page);
                if (partitionHashBucket != partition) {
                    continue;
                }

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

    private int getPartitionHashBucket(int position, Page page)
    {
        long hashCode = 1;
        for (int channel : partitioningChannels) {
            hashCode *= 31;
            Type type = types.get(channel);
            Block block = page.getBlock(channel);
            hashCode += hashPosition(type, block, position);
        }
        // clear the sign bit
        hashCode &= 0x7fff_ffff_ffff_ffffL;

        int bucket = (int) (hashCode % partitionCount);
        checkState(bucket >= 0 && bucket < partitionCount);
        return bucket;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(partition, partitionCount, partitioningChannels);
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
        final HashPagePartitionFunction other = (HashPagePartitionFunction) obj;
        return Objects.equal(this.partition, other.partition) &&
                Objects.equal(this.partitionCount, other.partitionCount) &&
                Objects.equal(this.partitioningChannels, other.partitioningChannels);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("partition", partition)
                .add("partitionCount", partitionCount)
                .add("partitioningChannels", partitioningChannels)
                .toString();
    }
}
