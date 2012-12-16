/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.operator.aggregation.AggregationFunctionStep;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.longs.Long2IntOpenCustomHashMap;
import it.unimi.dsi.fastutil.longs.LongHash.Strategy;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import javax.inject.Provider;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceOffset;
import static com.facebook.presto.operator.SyntheticAddress.encodeSyntheticAddress;
import static com.google.common.base.Preconditions.checkState;

public class GroupByAggregation
{
    private static final int LOOKUP_SLICE_INDEX = 0xFF_FF_FF_FF;

    private final ImmutableList<TupleInfo> tupleInfos;
    private final List<ObjectArrayList<AggregationFunctionStep>> aggregateValues;
    private final Iterator<UncompressedBlock> groupByBlocksIterator;

    private int currentPosition;

    public GroupByAggregation(Operator source,
            int groupChannel,
            int expectedGroups,
            OperatorStats operatorStats,
            List<Provider<AggregationFunctionStep>> functionProviders)
    {
        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (Provider<AggregationFunctionStep> functionProvider : functionProviders) {
            tupleInfos.add(functionProvider.get().getTupleInfo());
        }
        this.tupleInfos = tupleInfos.build();

        ImmutableList.Builder<ObjectArrayList<AggregationFunctionStep>> builder = ImmutableList.builder();
        for (int i = 0; i < functionProviders.size(); i++) {
            ObjectArrayList<AggregationFunctionStep> functions = ObjectArrayList.wrap(new AggregationFunctionStep[expectedGroups], 0);
            builder.add(functions);
        }
        aggregateValues = builder.build();


        TupleInfo groupByTupleInfo = source.getTupleInfos().get(groupChannel);
        SliceHashStrategy hashStrategy = new SliceHashStrategy(groupByTupleInfo);
        Long2IntOpenCustomHashMap addressToGroupId = new Long2IntOpenCustomHashMap(expectedGroups, hashStrategy);
        addressToGroupId.defaultReturnValue(-1);


        Slice slice = Slices.allocate((int) BlockBuilder.DEFAULT_MAX_BLOCK_SIZE.toBytes());
        hashStrategy.addSlice(slice);
        BlockBuilder blockBuilder = new BlockBuilder(groupByTupleInfo, slice.length(), slice.getOutput());

        int nextGroupId = 0;

        List<UncompressedBlock> groupByBlocks = new ArrayList<>();
        BlockCursor[] cursors = new BlockCursor[source.getChannelCount()];
        PageIterator iterator = source.iterator(operatorStats);
        while (iterator.hasNext()) {
            Page page = iterator.next();
            Block[] blocks = page.getBlocks();
            Slice groupBySlice = ((UncompressedBlock) blocks[groupChannel]).getSlice();
            hashStrategy.setLookupSlice(groupBySlice);

            for (int i = 0; i < blocks.length; i++) {
                cursors[i] = blocks[i].cursor();
            }

            int rows = page.getPositionCount();
            for (int position = 0; position < rows; position++) {
                for (BlockCursor cursor : cursors) {
                    checkState(cursor.advanceNextPosition());
                }

                int groupByRawOffset = cursors[groupChannel].getRawOffset();
                int groupId = addressToGroupId.get(encodeSyntheticAddress(LOOKUP_SLICE_INDEX, groupByRawOffset));
                if (groupId < 0) {
                    // new group

                    // copy group by tuple to hash
                    int length = groupByTupleInfo.size(groupBySlice, groupByRawOffset);
                    if (blockBuilder.writableBytes() < length) {
                        UncompressedBlock block = blockBuilder.build();
                        groupByBlocks.add(block);
                        slice = Slices.allocate(Math.max((int) BlockBuilder.DEFAULT_MAX_BLOCK_SIZE.toBytes(), length));
                        blockBuilder = new BlockBuilder(groupByTupleInfo, slice.length(), slice.getOutput());
                        hashStrategy.addSlice(slice);
                    }
                    blockBuilder.appendTuple(groupBySlice, groupByRawOffset, length);

                    // record group id in hash
                    groupId = nextGroupId++;
                    addressToGroupId.put(encodeSyntheticAddress(groupByBlocks.size() - 1, groupByRawOffset), groupId);

                    // create functions and process initial value
                    for (int i = 0; i < functionProviders.size(); i++) {
                        Provider<AggregationFunctionStep> provider = functionProviders.get(i);
                        AggregationFunctionStep function = provider.get();
                        aggregateValues.get(i).add(function);
                        function.add(cursors);
                    }
                }
                else {
                    // existing group
                    for (ObjectArrayList<AggregationFunctionStep> aggregateValue : aggregateValues) {
                        AggregationFunctionStep function = aggregateValue.get(groupId);
                        function.add(cursors);
                    }
                }
            }

            for (BlockCursor cursor : cursors) {
                checkState(!cursor.advanceNextPosition());
            }
        }
        groupByBlocksIterator = groupByBlocks.iterator();
    }

    protected Page nextPage()
    {
        // if no more data, return null
        if (!groupByBlocksIterator.hasNext()) {
            //endOfData();
            return null;
        }

        // todo pre calculate tuple infos
        BlockBuilder[] outputs = new BlockBuilder[tupleInfos.size()];
        for (int i = 0; i < outputs.length; i++) {
            outputs[i] = new BlockBuilder(tupleInfos.get(i));
        }

        UncompressedBlock groupByBlock = groupByBlocksIterator.next();
        for (int i = 0; i < groupByBlock.getPositionCount(); i++) {
            for (int channel = 0; channel < outputs.length; channel++) {
                outputs[channel].append(aggregateValues.get(channel).get(currentPosition).evaluate());
            }
            currentPosition++;
        }

        Block[] blocks = new Block[outputs.length + 1];
        blocks[0] = groupByBlock;
        for (int i = 0; i < blocks.length; i++) {
            blocks[i + 1] = outputs[i].build();
        }

        Page page = new Page(blocks);
        return page;
    }

    public static class SliceHashStrategy
            implements Strategy
    {
        private final TupleInfo tupleInfo;
        private final List<Slice> slices;
        private Slice lookupSlice;

        public SliceHashStrategy(TupleInfo tupleInfo)
        {
            this.tupleInfo = tupleInfo;
            this.slices = ObjectArrayList.wrap(new Slice[10240], 0);
        }

        public void setLookupSlice(Slice lookupSlice)
        {
            this.lookupSlice = lookupSlice;
        }

        public void addSlice(Slice slice)
        {
            slices.add(slice);
        }

        @Override
        public int hashCode(long sliceAddress)
        {
            Slice slice = getSliceForSyntheticAddress(sliceAddress);
            int offset = (int) sliceAddress;
            int length = tupleInfo.size(slice, offset);
            int hashCode = slice.hashCode(offset, length);
            return hashCode;
        }

        @Override
        public boolean equals(long leftSliceAddress, long rightSliceAddress)
        {
            Slice leftSlice = getSliceForSyntheticAddress(leftSliceAddress);
            int leftOffset = decodeSliceOffset(leftSliceAddress);
            int leftLength = tupleInfo.size(leftSlice, leftOffset);

            Slice rightSlice = getSliceForSyntheticAddress(rightSliceAddress);
            int rightOffset = decodeSliceOffset(rightSliceAddress);
            int rightLength = tupleInfo.size(rightSlice, rightOffset);

            return leftSlice.equals(leftOffset, leftLength, rightSlice, rightOffset, rightLength);

        }

        private Slice getSliceForSyntheticAddress(long sliceAddress)
        {
            int sliceIndex = decodeSliceIndex(sliceAddress);
            Slice slice;
            if (sliceIndex == LOOKUP_SLICE_INDEX) {
                slice = lookupSlice;
            }
            else {
                slice = slices.get(sliceIndex);
            }
            return slice;
        }
    }
}
