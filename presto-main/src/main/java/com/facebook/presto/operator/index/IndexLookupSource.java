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
package com.facebook.presto.operator.index;

import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.LookupSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;

import javax.annotation.concurrent.NotThreadSafe;

import static com.facebook.presto.operator.index.IndexSnapshot.UNLOADED_INDEX_KEY;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class IndexLookupSource
        implements LookupSource
{
    private final IndexLoader indexLoader;
    private IndexedData indexedData;
    private FilterFunction filterFunction;

    public IndexLookupSource(IndexLoader indexLoader)
    {
        this.indexLoader = requireNonNull(indexLoader, "indexLoader is null");
        this.indexedData = indexLoader.getIndexSnapshot();
    }

    @Override
    public int getChannelCount()
    {
        return indexLoader.getChannelCount();
    }

    @Override
    public int getJoinPositionCount()
    {
        throw new UnsupportedOperationException("Index can not be used in a RIGHT or FULL outer join");
    }

    @Override
    public long getInMemorySizeInBytes()
    {
        return 0;
    }

    @Override
    public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage, long rawHash)
    {
        // TODO update to take advantage of precomputed hash
        return getJoinPosition(position, hashChannelsPage, allChannelsPage);
    }

    @Override
    public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage)
    {
        Block[] blocks = hashChannelsPage.getBlocks();
        long joinPosition = indexedData.getJoinPosition(position, hashChannelsPage);
        if (joinPosition == UNLOADED_INDEX_KEY) {
            indexedData.close(); // Close out the old indexedData
            indexedData = indexLoader.getIndexedDataForKeys(position, blocks);
            joinPosition = indexedData.getJoinPosition(position, hashChannelsPage);
            checkState(joinPosition != UNLOADED_INDEX_KEY);
        }
        // INVARIANT: position is -1 or a valid position greater than or equal to zero
        return joinPosition;
    }

    @Override
    public long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
    {
        long nextPosition = indexedData.getNextJoinPosition(currentJoinPosition);
        checkState(nextPosition != UNLOADED_INDEX_KEY);
        // INVARIANT: currentPosition is -1 or a valid currentPosition greater than or equal to zero
        return nextPosition;
    }

    @Override
    public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        indexedData.appendTo(position, pageBuilder, outputChannelOffset);
    }

    @Override
    public void close()
    {
        indexedData.close();
    }
}
