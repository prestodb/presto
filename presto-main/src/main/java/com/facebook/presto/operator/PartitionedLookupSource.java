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

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.lang.Integer.numberOfTrailingZeros;
import static java.lang.Math.toIntExact;

@NotThreadSafe
public class PartitionedLookupSource
        implements LookupSource
{
    public static Supplier<LookupSource> createPartitionedLookupSourceSupplier(List<Supplier<LookupSource>> partitions, List<Type> hashChannelTypes, boolean outer)
    {
        Optional<OuterPositionTracker.Factory> outerPositionTrackerFactory = outer ?
                Optional.of(new OuterPositionTracker.Factory(
                        partitions.stream()
                                .map(partition -> partition.get().getJoinPositionCount())
                                .collect(toImmutableList())))
                : Optional.empty();

        return () -> new PartitionedLookupSource(
                partitions.stream()
                        .map(Supplier::get)
                        .collect(toImmutableList()),
                hashChannelTypes,
                outerPositionTrackerFactory.map(OuterPositionTracker.Factory::create));
    }

    private final LookupSource[] lookupSources;
    private final LocalPartitionGenerator partitionGenerator;
    private final int partitionMask;
    private final int shiftSize;
    @Nullable
    private final OuterPositionTracker outerPositionTracker;

    private PartitionedLookupSource(List<? extends LookupSource> lookupSources, List<Type> hashChannelTypes, Optional<OuterPositionTracker> outerPositionTracker)
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
        this.outerPositionTracker = outerPositionTracker.orElse(null);
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
    public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage)
    {
        return getJoinPosition(position, hashChannelsPage, allChannelsPage, partitionGenerator.getRawHash(position, hashChannelsPage));
    }

    @Override
    public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage, long rawHash)
    {
        int partition = partitionGenerator.getPartition(rawHash);
        LookupSource lookupSource = lookupSources[partition];
        long joinPosition = lookupSource.getJoinPosition(position, hashChannelsPage, allChannelsPage, rawHash);
        if (joinPosition < 0) {
            return joinPosition;
        }
        return encodePartitionedJoinPosition(partition, toIntExact(joinPosition));
    }

    @Override
    public long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
    {
        int partition = decodePartition(currentJoinPosition);
        long joinPosition = decodeJoinPosition(currentJoinPosition);
        LookupSource lookupSource = lookupSources[partition];
        long nextJoinPosition = lookupSource.getNextJoinPosition(joinPosition, probePosition, allProbeChannelsPage);
        if (nextJoinPosition < 0) {
            return nextJoinPosition;
        }
        return encodePartitionedJoinPosition(partition, toIntExact(nextJoinPosition));
    }

    @Override
    public void appendTo(long partitionedJoinPosition, PageBuilder pageBuilder, int outputChannelOffset)
    {
        int partition = decodePartition(partitionedJoinPosition);
        int joinPosition = decodeJoinPosition(partitionedJoinPosition);
        lookupSources[partition].appendTo(joinPosition, pageBuilder, outputChannelOffset);
        if (outerPositionTracker != null) {
            outerPositionTracker.positionVisited(partition, joinPosition);
        }
    }

    @Override
    public OuterPositionIterator getOuterPositionIterator()
    {
        checkState(outerPositionTracker != null, "This is not an outer lookup source");
        return new PartitionedLookupOuterPositionIterator(lookupSources, outerPositionTracker.getVisitedPositions());
    }

    @Override
    public void close()
    {
        if (outerPositionTracker != null) {
            outerPositionTracker.commit();
        }
    }

    private int decodePartition(long partitionedJoinPosition)
    {
        return (int) (partitionedJoinPosition & partitionMask);
    }

    private int decodeJoinPosition(long partitionedJoinPosition)
    {
        return toIntExact(partitionedJoinPosition >>> shiftSize);
    }

    private long encodePartitionedJoinPosition(int partition, int joinPosition)
    {
        return (((long) joinPosition) << shiftSize) | (partition);
    }

    private static class PartitionedLookupOuterPositionIterator
            implements OuterPositionIterator
    {
        private final LookupSource[] lookupSources;
        private final boolean[][] visitedPositions;

        @GuardedBy("this")
        private int currentSource;

        @GuardedBy("this")
        private int currentPosition;

        public PartitionedLookupOuterPositionIterator(LookupSource[] lookupSources, boolean[][] visitedPositions)
        {
            this.lookupSources = lookupSources;
            this.visitedPositions = visitedPositions;
        }

        @Override
        public synchronized boolean appendToNext(PageBuilder pageBuilder, int outputChannelOffset)
        {
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

    /**
     * Each LookupSource has it's own copy of OuterPositionTracker instance.
     * Each of those OuterPositionTracker must be committed after last write
     * and before first read.
     *
     * All instances share visitedPositions array, but it is safe because each thread
     * starts with visitedPositions filled with false values and marks only some positions
     * to true. Since we don't care what will be the order of those writes to
     * visitedPositions, writes can be without synchronization.
     *
     * Memory visibility between last writes in commit() and first read in
     * getVisitedPositions() is guaranteed by accessing AtomicLong referenceCount
     * variables in those two methods.
     */
    private static class OuterPositionTracker
    {
        public static class Factory
        {
            private final boolean[][] visitedPositions;
            private final AtomicLong referenceCount = new AtomicLong();

            public Factory(List<Integer> positionCounts)
            {
                visitedPositions = new boolean[positionCounts.size()][];
                for (int partition = 0; partition < visitedPositions.length; partition++) {
                    visitedPositions[partition] = new boolean[positionCounts.get(partition)];
                }
            }

            public OuterPositionTracker create()
            {
                return new OuterPositionTracker(visitedPositions, referenceCount);
            }
        }

        private final boolean[][] visitedPositions; // shared across multiple operators/drivers
        private final AtomicLong referenceCount; // shared across multiple operators/drivers
        private boolean written; // unique per each operator/driver

        private OuterPositionTracker(boolean[][] visitedPositions, AtomicLong referenceCount)
        {
            this.visitedPositions = visitedPositions;
            this.referenceCount = referenceCount;
        }

        /**
         * No synchronization here, because it would be very expensive. Check comment above.
         */
        public void positionVisited(int partition, int position)
        {
            if (!written) {
                written = true;
                incrementReferenceCount();
            }
            visitedPositions[partition][position] = true;
        }

        public void commit()
        {
            if (written) {
                // touching atomic values ensures memory visibility between commit and getVisitedPositions
                referenceCount.decrementAndGet();
            }
        }

        public boolean[][] getVisitedPositions()
        {
            // touching atomic values ensures memory visibility between commit and getVisitedPositions
            verify(referenceCount.get() == 0);
            return visitedPositions;
        }

        private void incrementReferenceCount()
        {
            referenceCount.incrementAndGet();
        }
    }
}
