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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public final class OuterLookupSource
        implements LookupSource
{
    public static Supplier<LookupSource> createOuterLookupSourceSupplier(Supplier<LookupSource> lookupSourceSupplier)
    {
        return new OuterLookupSourceSupplier(lookupSourceSupplier);
    }

    private final LookupSource lookupSource;
    private final OuterPositionTracker outerPositionTracker;

    private OuterLookupSource(LookupSource lookupSource, OuterPositionTracker outerPositionTracker)
    {
        this.lookupSource = requireNonNull(lookupSource, "lookupSource is null");
        this.outerPositionTracker = requireNonNull(outerPositionTracker, "outerPositionTracker is null");
    }

    @Override
    public int getChannelCount()
    {
        return lookupSource.getChannelCount();
    }

    @Override
    public int getJoinPositionCount()
    {
        return lookupSource.getJoinPositionCount();
    }

    @Override
    public long getInMemorySizeInBytes()
    {
        return lookupSource.getInMemorySizeInBytes();
    }

    @Override
    public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage, long rawHash)
    {
        return lookupSource.getJoinPosition(position, hashChannelsPage, allChannelsPage, rawHash);
    }

    @Override
    public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage)
    {
        return lookupSource.getJoinPosition(position, hashChannelsPage, allChannelsPage);
    }

    @Override
    public long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
    {
        return lookupSource.getNextJoinPosition(currentJoinPosition, probePosition, allProbeChannelsPage);
    }

    @Override
    public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        lookupSource.appendTo(position, pageBuilder, outputChannelOffset);
        outerPositionTracker.positionVisited(position);
    }

    @Override
    public OuterPositionIterator getOuterPositionIterator()
    {
        return outerPositionTracker.getOuterPositionIterator();
    }

    @Override
    public void close()
    {
        lookupSource.close();
    }

    @ThreadSafe
    private static class SharedLookupOuterPositionIterator
            implements OuterPositionIterator
    {
        private final LookupSource lookupSource;
        private final boolean[] visitedPositions;

        @GuardedBy("this")
        private int currentPosition;

        public SharedLookupOuterPositionIterator(LookupSource lookupSource, boolean[] visitedPositions)
        {
            this.lookupSource = requireNonNull(lookupSource, "lookupSource is null");
            this.visitedPositions = requireNonNull(visitedPositions, "visitedPositions is null");
            checkArgument(lookupSource.getJoinPositionCount() == visitedPositions.length);
        }

        @Override
        public synchronized boolean appendToNext(PageBuilder pageBuilder, int outputChannelOffset)
        {
            while (currentPosition < visitedPositions.length) {
                if (!visitedPositions[currentPosition]) {
                    lookupSource.appendTo(currentPosition, pageBuilder, outputChannelOffset);
                    currentPosition++;
                    return true;
                }
                currentPosition++;
            }
            return false;
        }
    }

    @ThreadSafe
    private static class OuterLookupSourceSupplier
            implements Supplier<LookupSource>
    {
        private final Supplier<LookupSource> lookupSourceSupplier;
        private final OuterPositionTracker outerPositionTracker;

        public OuterLookupSourceSupplier(Supplier<LookupSource> lookupSourceSupplier)
        {
            this.lookupSourceSupplier = requireNonNull(lookupSourceSupplier, "lookupSourceSupplier is null");
            this.outerPositionTracker = new OuterPositionTracker(lookupSourceSupplier);
        }

        @Override
        public LookupSource get()
        {
            return new OuterLookupSource(lookupSourceSupplier.get(), outerPositionTracker);
        }
    }

    @ThreadSafe
    private static class OuterPositionTracker
    {
        private final Supplier<LookupSource> lookupSourceSupplier;

        @GuardedBy("this")
        private final boolean[] visitedPositions;

        @GuardedBy("this")
        private boolean finished;

        public OuterPositionTracker(Supplier<LookupSource> lookupSourceSupplier)
        {
            this.lookupSourceSupplier = lookupSourceSupplier;

            try (LookupSource lookupSource = lookupSourceSupplier.get()) {
                this.visitedPositions = new boolean[lookupSource.getJoinPositionCount()];
            }
        }

        public synchronized void positionVisited(long position)
        {
            verify(!finished);
            visitedPositions[toIntExact(position)] = true;
        }

        public synchronized OuterPositionIterator getOuterPositionIterator()
        {
            finished = true;
            return new SharedLookupOuterPositionIterator(lookupSourceSupplier.get(), visitedPositions);
        }
    }
}
