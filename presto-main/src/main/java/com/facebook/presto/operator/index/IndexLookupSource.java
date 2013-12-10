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

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.LookupSource;
import com.facebook.presto.operator.PageBuilder;

import javax.annotation.concurrent.NotThreadSafe;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@NotThreadSafe
public class IndexLookupSource
        implements LookupSource
{
    private final IndexLoader indexLoader;
    private IndexSnapshotReader indexReader;

    public IndexLookupSource(IndexLoader indexLoader)
    {
        this.indexLoader = checkNotNull(indexLoader, "indexLoader is null");
        indexReader = indexLoader.getReader();
    }

    @Override
    public int getChannelCount()
    {
        return indexLoader.getChannelCount();
    }

    @Override
    public long getJoinPosition(BlockCursor[] joinCursors)
    {
        long position = indexReader.getJoinPosition(joinCursors);
        if (position == IndexSnapshotReader.UNLOADED_INDEX_KEY) {
            indexReader = indexLoader.getUpdatedReader(joinCursors);
            position = indexReader.getJoinPosition(joinCursors);
        }
        checkState(position != IndexSnapshotReader.UNLOADED_INDEX_KEY);
        // INVARIANT: position is -1 or a valid position greater than or equal to zero
        return position;
    }

    @Override
    public long getNextJoinPosition(long position)
    {
        long nextPosition = indexReader.getNextJoinPosition(position);
        checkState(nextPosition != IndexSnapshotReader.UNLOADED_INDEX_KEY);
        // INVARIANT: position is -1 or a valid position greater than or equal to zero
        return nextPosition;
    }

    @Override
    public void appendTupleTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        indexReader.appendTupleTo(position, pageBuilder, outputChannelOffset);
    }
}
