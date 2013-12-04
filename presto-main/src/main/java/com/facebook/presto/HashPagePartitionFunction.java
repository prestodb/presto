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

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.tuple.TupleInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public final class HashPagePartitionFunction
        implements PagePartitionFunction
{
    private final int partition;
    private final int partitionCount;
    private final int partitioningChannel;

    @JsonCreator
    public HashPagePartitionFunction(
            @JsonProperty("partition") int partition,
            @JsonProperty("partitionCount") int partitionCount,
            @JsonProperty("partitioningChannel") int partitioningChannel)
    {
        this.partition = partition;
        this.partitionCount = partitionCount;
        this.partitioningChannel = partitioningChannel;
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
    public int getPartitioningChannel()
    {
        return partitioningChannel;
    }

    @Override
    public List<Page> partition(List<Page> pages)
    {
        if (pages.isEmpty()) {
            return pages;
        }

        List<TupleInfo> tupleInfos = getTupleInfos(pages);

        PageBuilder pageBuilder = new PageBuilder(tupleInfos);

        ImmutableList.Builder<Page> partitionedPages = ImmutableList.builder();
        for (Page page : pages) {
            // open the page
            BlockCursor[] cursors = new BlockCursor[tupleInfos.size()];
            for (int i = 0; i < cursors.length; i++) {
                cursors[i] = page.getBlock(i).cursor();
            }
            // for each position
            for (int position = 0; position < page.getPositionCount(); position++) {
                // advance all cursors
                for (BlockCursor cursor : cursors) {
                    cursor.advanceNextPosition();
                }

                // if hash is not in range skip
                int partitionHashBucket = getPartitionHashBucket(tupleInfos, cursors);
                if (partitionHashBucket != partition) {
                    continue;
                }

                // append row
                for (int channel = 0; channel < cursors.length; channel++) {
                    pageBuilder.getBlockBuilder(channel).append(cursors[channel]);
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

    private int getPartitionHashBucket(List<TupleInfo> tupleInfos, BlockCursor[] cursors)
    {
        long hashCode = getPartitionedHashCode(tupleInfos, cursors[partitioningChannel]) & 0xFFFF_FFFFL;
        int bucket = (int) (hashCode % partitionCount);
        checkState(bucket >= 0 && bucket < partitionCount);
        return bucket;
    }

    private int getPartitionedHashCode(List<TupleInfo> tupleInfos, BlockCursor partitionedCursor)
    {
        Slice partitionedSlice = partitionedCursor.getRawSlice();
        int partitionedOffset = partitionedCursor.getRawOffset();
        int partitionedLength = tupleInfos.get(partitioningChannel).size(partitionedSlice, partitionedOffset);
        return partitionedSlice.hashCode(partitionedOffset, partitionedLength);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(partition, partitionCount, partitioningChannel);
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
                Objects.equal(this.partitioningChannel, other.partitioningChannel);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("partition", partition)
                .add("partitionCount", partitionCount)
                .add("partitioningChannel", partitioningChannel)
                .toString();
    }

    private static List<TupleInfo> getTupleInfos(List<Page> pages)
    {
        Page firstPage = pages.get(0);
        List<TupleInfo> tupleInfos = new ArrayList<>();
        for (Block block : firstPage.getBlocks()) {
            tupleInfos.add(block.getTupleInfo());
        }
        return tupleInfos;
    }
}

