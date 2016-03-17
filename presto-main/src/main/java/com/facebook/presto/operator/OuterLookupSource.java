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
import com.google.common.primitives.Ints;

import javax.annotation.concurrent.GuardedBy;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class OuterLookupSource
        implements LookupSource
{
    private final LookupSource lookupSource;

    @GuardedBy("this")
    private final boolean[] visitedPositions;

    public OuterLookupSource(LookupSource lookupSource)
    {
        this.lookupSource = requireNonNull(lookupSource, "lookupSource is null");
        this.visitedPositions = new boolean[lookupSource.getJoinPositionCount()];
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
        synchronized (this) {
            visitedPositions[Ints.checkedCast(position)] = true;
        }
    }

    @Override
    public synchronized OuterPositionIterator getOuterPositionIterator()
    {
        return new SharedLookupOuterPositionIterator(lookupSource, visitedPositions);
    }

    @Override
    public void close()
    {
        // this method only exists for index lookup which does not support build outer
    }

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
}
