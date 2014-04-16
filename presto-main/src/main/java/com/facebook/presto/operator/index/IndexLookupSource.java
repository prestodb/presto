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

import com.facebook.presto.operator.LookupSource;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.spi.block.BlockCursor;

import javax.annotation.concurrent.NotThreadSafe;

import static com.facebook.presto.operator.index.IndexSnapshot.UNLOADED_INDEX_KEY;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@NotThreadSafe
public class IndexLookupSource
        implements LookupSource
{
    private final IndexLoader indexLoader;
    private IndexSnapshot indexSnapshot;

    public IndexLookupSource(IndexLoader indexLoader)
    {
        this.indexLoader = checkNotNull(indexLoader, "indexLoader is null");
        indexSnapshot = indexLoader.getIndexSnapshot();
    }

    @Override
    public int getChannelCount()
    {
        return indexLoader.getChannelCount();
    }

    @Override
    public long getJoinPosition(BlockCursor... cursors)
    {
        long position = indexSnapshot.getJoinPosition(cursors);
        if (position == UNLOADED_INDEX_KEY) {
            indexSnapshot = indexLoader.getIndexSnapshotForKeys(cursors);
            position = indexSnapshot.getJoinPosition(cursors);
            if (position == UNLOADED_INDEX_KEY) {
                checkState(position != UNLOADED_INDEX_KEY);
            }
        }
        // INVARIANT: position is -1 or a valid position greater than or equal to zero
        return position;
    }

    @Override
    public long getNextJoinPosition(long currentPosition)
    {
        long nextPosition = indexSnapshot.getNextJoinPosition(currentPosition);
        checkState(nextPosition != UNLOADED_INDEX_KEY);
        // INVARIANT: currentPosition is -1 or a valid currentPosition greater than or equal to zero
        return nextPosition;
    }

    @Override
    public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        indexSnapshot.appendTo(position, pageBuilder, outputChannelOffset);
    }
}
