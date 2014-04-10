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
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.GroupByHash;
import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.operator.HashAggregationOperator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PipelineContext;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntListIterator;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class UnloadedIndexKeyRecordSet
        implements RecordSet
{
    private final List<TupleInfo.Type> types;
    private final List<PageAndPositions> pageAndPositions;

    public UnloadedIndexKeyRecordSet(IndexSnapshot existingSnapshot, List<TupleInfo.Type> types, PipelineContext pipelineContext, List<Page> pages)
    {
        checkNotNull(existingSnapshot, "existingSnapshot is null");
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        checkNotNull(pipelineContext, "pipelineContext is null");
        checkNotNull(pages, "pages is null");

        // Get a context for computing this record set
        DriverContext driverContext = pipelineContext.addDriverContext();
        OperatorContext operatorContext = driverContext.addOperatorContext(0, UnloadedIndexKeyRecordSet.class.getSimpleName());

        // Distinct is essentially a group by on all channels
        int[] allChannels = new int[types.size()];
        for (int i = 0; i < allChannels.length; i++) {
            allChannels[i] = i;
        }

        ImmutableList.Builder<PageAndPositions> builder = ImmutableList.builder();
        long nextDistinctId = 0;
        GroupByHash groupByHash = new GroupByHash(types, allChannels, 10_000, new HashAggregationOperator.HashMemoryManager(operatorContext));
        IndexKey mutableIndexKey = IndexKey.createMutableJoinCursorKey();
        for (Page page : pages) {
            GroupByIdBlock groupIds = groupByHash.getGroupIds(page);
            IntList positions = new IntArrayList();

            BlockCursor[] cursors = new BlockCursor[page.getChannelCount()];
            for (int i = 0; i < page.getChannelCount(); i++) {
                cursors[i] = page.getBlock(i).cursor();
            }

            // Move through the positions while advancing the cursors in lockstep
            for (int position = 0; position < groupIds.getPositionCount(); position++) {
                for (int i = 0; i < cursors.length; i++) {
                    checkState(cursors[i].advanceNextPosition());
                }

                // Find all distinct keys
                if (groupIds.getGroupId(position) == nextDistinctId) {
                    nextDistinctId++;

                    // Only include the key if it is not already in the index
                    if (existingSnapshot.getAddress(mutableIndexKey.setJoinCursors(cursors)) == -1L) {
                        positions.add(position);
                    }
                }
            }
            if (!positions.isEmpty()) {
                builder.add(new PageAndPositions(page, positions));
            }
        }

        // Mark this driver context as finished to wipe the stats and memory consumption
        pipelineContext.driverFinished(driverContext);

        pageAndPositions = builder.build();
    }

    @Override
    public List<ColumnType> getColumnTypes()
    {
        return Lists.transform(types, new Function<TupleInfo.Type, ColumnType>()
        {
            @Override
            public ColumnType apply(TupleInfo.Type type)
            {
                return type.toColumnType();
            }
        });
    }

    @Override
    public UnloadedIndexKeyRecordCursor cursor()
    {
        return new UnloadedIndexKeyRecordCursor(types, pageAndPositions);
    }

    public static class UnloadedIndexKeyRecordCursor
            implements RecordCursor
    {
        private final List<TupleInfo.Type> types;
        private final Iterator<PageAndPositions> pageAndPositionsIterator;
        private final BlockCursor[] cursors;
        private IntListIterator positionIterator;

        public UnloadedIndexKeyRecordCursor(List<TupleInfo.Type> types, List<PageAndPositions> pageAndPositions)
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
        public ColumnType getType(int field)
        {
            TupleInfo.Type type = types.get(field);
            switch (type) {
                case BOOLEAN:
                    return ColumnType.BOOLEAN;
                case DOUBLE:
                    return ColumnType.DOUBLE;
                case FIXED_INT_64:
                    return ColumnType.LONG;
                case VARIABLE_BINARY:
                    return ColumnType.STRING;
                default:
                    throw new AssertionError("Unknown type: " + type);
            }
        }

        @Override
        public boolean advanceNextPosition()
        {
            while (positionIterator == null || !positionIterator.hasNext()) {
                if (!pageAndPositionsIterator.hasNext()) {
                    return false;
                }
                PageAndPositions pageAndPositions = pageAndPositionsIterator.next();
                checkState(types.size() == pageAndPositions.getPage().getChannelCount());
                for (int i = 0; i < pageAndPositions.getPage().getChannelCount(); i++) {
                    cursors[i] = pageAndPositions.getPage().getBlock(i).cursor();
                }
                positionIterator = pageAndPositions.getPositions().iterator();
            }

            int position = positionIterator.nextInt();
            for (int i = 0; i < cursors.length; i++) {
                checkState(cursors[i].advanceToPosition(position));
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
        private final Page page;
        private final IntList positions;

        private PageAndPositions(Page page, IntList positions)
        {
            this.page = checkNotNull(page, "page is null");
            this.positions = checkNotNull(positions, "positions is null");
        }

        private Page getPage()
        {
            return page;
        }

        private IntList getPositions()
        {
            return positions;
        }
    }
}
