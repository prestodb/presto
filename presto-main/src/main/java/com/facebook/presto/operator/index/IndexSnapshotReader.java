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
import com.facebook.presto.operator.PageBuilder;

import javax.annotation.concurrent.NotThreadSafe;

import static com.google.common.base.Preconditions.checkNotNull;

@NotThreadSafe
public class IndexSnapshotReader
{
    public static final long UNLOADED_INDEX_KEY = -2;
    public static final long NO_MORE_POSITIONS = -1;

    private final IndexSnapshot snapshot;
    private final IndexKey mutableIndexKey =  IndexKey.createMutableJoinCursorKey();

    public IndexSnapshotReader(IndexSnapshot snapshot)
    {
        this.snapshot = checkNotNull(snapshot, "snapshot is null");
    }

    /**
     * Returns UNLOADED_INDEX_KEY if the key has not been loaded.
     * Returns NO_MORE_POSITIONS if the key has been loaded, but has no values.
     * Returns a valid address if the key has been loaded and has values.
     */
    public long getJoinPosition(BlockCursor[] joinCursors)
    {
        long address = snapshot.getAddress(mutableIndexKey.setJoinCursors(joinCursors));
        if (address == -1L) {
            return UNLOADED_INDEX_KEY;
        }

        int fragmentId = IndexAddress.decodeFragmentId(address);
        if (fragmentId == -1) {
            return NO_MORE_POSITIONS;
        }

        return address;
    }

    /**
     * Returns the next address to join.
     * Returns NO_MORE_POSITIONS if there are no more values to join.
     */
    public long getNextJoinPosition(long address)
    {
        int fragmentId = IndexAddress.decodeFragmentId(address);
        IndexSnapshot.Fragment fragment = snapshot.getFragment(fragmentId);

        int oldPosition = IndexAddress.decodePosition(address);
        int newPosition = fragment.getNextPosition(oldPosition);
        if (newPosition == -1) {
            return NO_MORE_POSITIONS;
        }
        return IndexAddress.encodeAddress(fragmentId, newPosition);
    }

    public void appendTupleTo(long address, PageBuilder pageBuilder, int outputChannelOffset)
    {
        int fragmentId = IndexAddress.decodeFragmentId(address);
        IndexSnapshot.Fragment fragment = snapshot.getFragment(fragmentId);

        int position = IndexAddress.decodePosition(address);
        fragment.appendTupleTo(position, pageBuilder, outputChannelOffset);
    }
}
