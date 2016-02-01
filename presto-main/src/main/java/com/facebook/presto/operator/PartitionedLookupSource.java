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

import com.facebook.presto.operator.exchange.LocalPartitionGenerator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.primitives.Ints;

import javax.annotation.concurrent.GuardedBy;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Integer.numberOfTrailingZeros;

public class PartitionedLookupSource
        implements LookupSource
{
    private final LookupSource[] lookupSources;
    private final LocalPartitionGenerator partitionGenerator;
    private final int partitionMask;
    private final int shiftSize;

    @GuardedBy("this")
    private final boolean[][] visitedPositions;

    public PartitionedLookupSource(List<? extends LookupSource> lookupSources, List<Type> hashChannelTypes, boolean outer)
    {
        this.lookupSources = lookupSources.toArray(new LookupSource[lookupSources.size()]);

        // this generator is only used for getJoinPosition without a rawHash and in this case
        // the hash channels are always packed in a page without extra columns
        int[] hashChannels = new int[hashChannelTypes.size()];
        for (int i = 0; i < hashChannels.length; i++) {
            hashChannels[i] = i;
        }
        this.partitionGenerator = new LocalPartitionGenerator(new InterpretedHashGenerator(hashChannelTypes, hashChannels), lookupSources.size());

        this.partitionMask = lookupSources.size() - 1;
        this.shiftSize = numberOfTrailingZeros(lookupSources.size()) + 1;

        if (outer) {
            visitedPositions = new boolean[lookupSources.size()][];
            for (int source = 0; source < this.lookupSources.length; source++) {
                visitedPositions[source] = new boolean[this.lookupSources[source].getJoinPositionCount()];
            }
        }
        else {
            visitedPositions = null;
        }
    }

    @Override
    public int getChannelCount()
    {
        return lookupSources[0].getChannelCount();
    }

    @Override
    public int getJoinPositionCount()
    {
        throw new UnsupportedOperationException("Parallel hash can not be used in a RIGHT or FULL outer join");
    }

    @Override
    public long getInMemorySizeInBytes()
    {
        return Arrays.stream(lookupSources).mapToLong(LookupSource::getInMemorySizeInBytes).sum();
    }

    @Override
    public long getJoinPosition(int position, Page page)
    {
        return getJoinPosition(position, page, partitionGenerator.getRawHash(position, page));
    }

    @Override
    public long getJoinPosition(int position, Page page, long rawHash)
    {
        int partition = partitionGenerator.getPartition(rawHash);
        LookupSource lookupSource = lookupSources[partition];
        long joinPosition = lookupSource.getJoinPosition(position, page, rawHash);
        if (joinPosition < 0) {
            return joinPosition;
        }
        return encodePartitionedJoinPosition(partition, Ints.checkedCast(joinPosition));
    }

    @Override
    public long getNextJoinPosition(long partitionedJoinPosition)
    {
        int partition = decodePartition(partitionedJoinPosition);
        long joinPosition = decodeJoinPosition(partitionedJoinPosition);
        LookupSource lookupSource = lookupSources[partition];
        long nextJoinPosition = lookupSource.getNextJoinPosition(joinPosition);
        if (nextJoinPosition < 0) {
            return nextJoinPosition;
        }
        return encodePartitionedJoinPosition(partition, Ints.checkedCast(nextJoinPosition));
    }

    @Override
    public void appendTo(long partitionedJoinPosition, PageBuilder pageBuilder, int outputChannelOffset)
    {
        int partition = decodePartition(partitionedJoinPosition);
        int joinPosition = decodeJoinPosition(partitionedJoinPosition);
        lookupSources[partition].appendTo(joinPosition, pageBuilder, outputChannelOffset);
        if (visitedPositions != null) {
            visitedPositions[partition][joinPosition] = true;
        }
    }

    @Override
    public OuterPositionIterator getOuterPositionIterator()
    {
        return new PartitionedLookupOuterPositionIterator();
    }

    @Override
    public void close()
    {
        // this method only exists for index lookup which does not support partitioned hash build (since it doesn't build)
    }

    private int decodePartition(long partitionedJoinPosition)
    {
        return (int) (partitionedJoinPosition & partitionMask);
    }

    private int decodeJoinPosition(long partitionedJoinPosition)
    {
        return Ints.checkedCast(partitionedJoinPosition >>> shiftSize);
    }

    private long encodePartitionedJoinPosition(int partition, int joinPosition)
    {
        return (joinPosition << shiftSize) | (partition);
    }

    private class PartitionedLookupOuterPositionIterator
            implements OuterPositionIterator
    {
        @GuardedBy("this")
        private int currentSource;

        @GuardedBy("this")
        private int currentPosition;

        public PartitionedLookupOuterPositionIterator()
        {
            checkState(visitedPositions != null, "This is not an outer lookup source");
        }

        @Override
        public boolean appendToNext(PageBuilder pageBuilder, int outputChannelOffset)
        {
            synchronized (PartitionedLookupSource.this) {
                while (currentSource < lookupSources.length) {
                    while (currentPosition < visitedPositions[currentSource].length) {
                        if (!visitedPositions[currentSource][currentPosition]) {
                            lookupSources[currentSource].appendTo(currentPosition, pageBuilder, outputChannelOffset);
                            currentPosition++;
                            return true;
                        }
                        currentPosition++;
                    }
                    currentPosition = 0;
                    currentSource++;
                }
                return false;
            }
        }
    }
}
