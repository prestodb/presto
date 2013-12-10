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

import com.facebook.presto.operator.GroupByHash;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntListIterator;

import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.operator.index.IndexSnapshot.UNLOADED_INDEX_KEY;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class UnloadedIndexKeyRecordSet
        implements RecordSet
{
    private final List<Type> types;
    private final List<PageAndPositions> pageAndPositions;

    public UnloadedIndexKeyRecordSet(IndexSnapshot existingSnapshot, List<Type> types, List<UpdateRequest> requests)
    {
        checkNotNull(existingSnapshot, "existingSnapshot is null");
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        checkNotNull(requests, "requests is null");

        // Distinct is essentially a group by on all channels
        int[] allChannels = new int[types.size()];
        for (int i = 0; i < allChannels.length; i++) {
            allChannels[i] = i;
        }

        ImmutableList.Builder<PageAndPositions> builder = ImmutableList.builder();
        long nextDistinctId = 0;
        GroupByHash groupByHash = new GroupByHash(types, allChannels, 10_000);
        for (UpdateRequest request : requests) {
            IntList positions = new IntArrayList();

            BlockCursor[] cursors = request.duplicateCursors();

            // Move through the positions while advancing the cursors in lockstep
            int positionCount = cursors[0].getRemainingPositions() + 1;
            for (int index = 0; index < positionCount; index++) {
                // cursors are start at valid position, so we don't need to advance the first time
                if (index > 0) {
                    for (BlockCursor cursor : cursors) {
                        checkState(cursor.advanceNextPosition());
                    }
                }

                if (groupByHash.putIfAbsent(cursors) == nextDistinctId) {
                    nextDistinctId++;

                    // Only include the key if it is not already in the index
                    if (existingSnapshot.getJoinPosition(cursors) == UNLOADED_INDEX_KEY) {
                        positions.add(cursors[0].getPosition());
                    }
                }

            }

            if (!positions.isEmpty()) {
                builder.add(new PageAndPositions(request, positions));
            }
        }

        pageAndPositions = builder.build();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return types;
    }

    @Override
    public UnloadedIndexKeyRecordCursor cursor()
    {
        return new UnloadedIndexKeyRecordCursor(types, pageAndPositions);
    }

    public static class UnloadedIndexKeyRecordCursor
            implements RecordCursor
    {
        private final List<Type> types;
        private final Iterator<PageAndPositions> pageAndPositionsIterator;
        private BlockCursor[] cursors;
        private IntListIterator positionIterator;

        public UnloadedIndexKeyRecordCursor(List<Type> types, List<PageAndPositions> pageAndPositions)
        {
            this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
            this.pageAndPositionsIterator = checkNotNull(pageAndPositions, "pageAndPositions is null").iterator();
            this.cursors = new BlockCursor[types.size()];
        }

        @Override
        public long getTotalBytes()
        {
            return 0;
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public Type getType(int field)
        {
            return types.get(field);
        }

        @Override
        public boolean advanceNextPosition()
        {
            while (positionIterator == null || !positionIterator.hasNext()) {
                if (!pageAndPositionsIterator.hasNext()) {
                    return false;
                }
                PageAndPositions pageAndPositions = pageAndPositionsIterator.next();
                cursors = pageAndPositions.getUpdateRequest().duplicateCursors();
                checkState(types.size() == cursors.length);
                positionIterator = pageAndPositions.getPositions().iterator();
            }

            int position = positionIterator.nextInt();
            for (BlockCursor cursor : cursors) {
                checkState(cursor.advanceToPosition(position));
            }

            return true;
        }

        public BlockCursor[] asBlockCursors()
        {
            return cursors;
        }

        @Override
        public boolean getBoolean(int field)
        {
            return cursors[field].getBoolean();
        }

        @Override
        public long getLong(int field)
        {
            return cursors[field].getLong();
        }

        @Override
        public double getDouble(int field)
        {
            return cursors[field].getDouble();
        }

        @Override
        public byte[] getString(int field)
        {
            return cursors[field].getSlice().getBytes();
        }

        @Override
        public boolean isNull(int field)
        {
            return cursors[field].isNull();
        }

        @Override
        public void close()
        {
            // Do nothing
        }
    }

    private static class PageAndPositions
    {
        private final UpdateRequest updateRequest;
        private final IntList positions;

        private PageAndPositions(UpdateRequest updateRequest, IntList positions)
        {
            this.updateRequest = checkNotNull(updateRequest, "updateRequest is null");
            this.positions = checkNotNull(positions, "positions is null");
        }

        private UpdateRequest getUpdateRequest()
        {
            return updateRequest;
        }

        private IntList getPositions()
        {
            return positions;
        }
    }
}
