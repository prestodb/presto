package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.tuple.TupleInfo;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.longs.LongHash;
import it.unimi.dsi.fastutil.longs.LongOpenCustomHashSet;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.List;

import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceOffset;
import static com.facebook.presto.operator.SyntheticAddress.encodeSyntheticAddress;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;

public class ChannelSet
{
    private static final int LOOKUP_SLICE_INDEX = 0xFF_FF_FF_FF;

    private final SliceHashStrategy strategy;
    private final AddressValueSet addressValueSet;
    private final boolean containsNull;

    public ChannelSet(PageIterator source, int setChannel, int expectedPositions, TaskMemoryManager taskMemoryManager, OperatorStats operatorStats)
    {
        checkNotNull(source, "source is null");
        checkArgument(setChannel >= 0, "setChannel must be greater than or equal to zero");
        checkArgument(expectedPositions >= 0, "expectedPositions must be greater than or equal to zero");
        checkNotNull(taskMemoryManager, "taskMemoryManager is null");
        checkNotNull(operatorStats, "operatorStats is null");

        // Construct the set from the source
        TupleInfo tupleInfo = source.getTupleInfos().get(setChannel);
        strategy = new SliceHashStrategy(tupleInfo);
        addressValueSet = new AddressValueSet(expectedPositions, strategy);

        // allocate the first slice of the set
        Slice slice = Slices.allocate((int) BlockBuilder.DEFAULT_MAX_BLOCK_SIZE.toBytes());
        strategy.addSlice(slice);
        BlockBuilder blockBuilder = new BlockBuilder(tupleInfo, slice.length(), slice.getOutput());

        int currentBlockId = 0;
        long currentSize = 0;
        boolean containsNull = false;
        try (PageIterator pageIterator = source) {
            while (pageIterator.hasNext() && !operatorStats.isDone()) {
                currentSize = taskMemoryManager.updateOperatorReservation(currentSize, getEstimatedSize());

                Block sourceBlock = pageIterator.next().getBlock(setChannel);
                BlockCursor sourceCursor = sourceBlock.cursor();
                Slice sourceSlice = ((UncompressedBlock) sourceBlock).getSlice();
                strategy.setLookupSlice(sourceSlice);

                for (int position = 0; position < sourceBlock.getPositionCount(); position++) {
                    checkState(sourceCursor.advanceNextPosition());
                    containsNull |= tupleContainsNull(sourceCursor, tupleInfo.getFieldCount());

                    long sourceAddress = encodeSyntheticAddress(LOOKUP_SLICE_INDEX, sourceCursor.getRawOffset());

                    if (!addressValueSet.contains(sourceAddress)) {
                        int length = tupleInfo.size(sourceSlice, sourceCursor.getRawOffset());
                        if (blockBuilder.writableBytes() < length) {
                            slice = Slices.allocate(Math.max((int) BlockBuilder.DEFAULT_MAX_BLOCK_SIZE.toBytes(), length));
                            strategy.addSlice(slice);
                            blockBuilder = new BlockBuilder(tupleInfo, slice.length(), slice.getOutput());
                            currentBlockId++;
                        }
                        int blockRawOffset = blockBuilder.size();
                        blockBuilder.appendTuple(sourceSlice, sourceCursor.getRawOffset(), length);
                        addressValueSet.add(encodeSyntheticAddress(currentBlockId, blockRawOffset));
                    }
                }
            }
        }
        this.containsNull = containsNull;
    }

    public ChannelSet(ChannelSet channelSet)
    {
        checkNotNull(channelSet, "channelSet is null");
        this.strategy = new SliceHashStrategy(channelSet.strategy);
        this.addressValueSet = new AddressValueSet(channelSet.addressValueSet, strategy);
        this.containsNull = channelSet.containsNull;
    }

    private static boolean tupleContainsNull(BlockCursor cursor, int fieldCount)
    {
        boolean containsNull = false;
        for (int i = 0; i < fieldCount; i++) {
            containsNull |= cursor.isNull(i);
        }
        return containsNull;
    }

    public boolean containsNull()
    {
        return containsNull;
    }

    public DataSize getEstimatedSize()
    {
        return new DataSize(addressValueSet.getEstimatedSize().toBytes() + strategy.getEstimatedSize().toBytes(), DataSize.Unit.BYTE);
    }

    public void setLookupSlice(Slice lookupSlice)
    {
        strategy.setLookupSlice(lookupSlice);
    }

    public boolean contains(BlockCursor cursor)
    {
        return addressValueSet.contains(SyntheticAddress.encodeSyntheticAddress(LOOKUP_SLICE_INDEX, cursor.getRawOffset()));
    }

    private static class AddressValueSet
            extends LongOpenCustomHashSet
    {
        private AddressValueSet(int expected, LongHash.Strategy strategy)
        {
            super(expected, strategy);
        }

        private AddressValueSet(AddressValueSet addressValueSet, LongHash.Strategy strategy)
        {
            super(addressValueSet, strategy);
        }

        public DataSize getEstimatedSize()
        {
            return new DataSize(sizeOf(this.key) + sizeOf(this.used), DataSize.Unit.BYTE);
        }
    }

    public static class SliceHashStrategy
            implements LongHash.Strategy
    {
        private final TupleInfo tupleInfo;
        private final List<Slice> slices;
        private Slice lookupSlice;
        private long memorySize;

        public SliceHashStrategy(TupleInfo tupleInfo)
        {
            this.tupleInfo = checkNotNull(tupleInfo, "tupleInfo is null");
            this.slices = ObjectArrayList.wrap(new Slice[1024], 0);
        }

        public SliceHashStrategy(SliceHashStrategy strategy)
        {
            checkNotNull(strategy, "strategy is null");
            this.tupleInfo = strategy.tupleInfo;
            this.slices = strategy.slices;
        }

        public DataSize getEstimatedSize()
        {
            return new DataSize(memorySize, DataSize.Unit.BYTE);
        }

        public void setLookupSlice(Slice lookupSlice)
        {
            checkNotNull(lookupSlice, "lookupSlice is null");
            this.lookupSlice = lookupSlice;
        }

        public void addSlice(Slice slice)
        {
            memorySize += slice.length();
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
            return sliceIndex == LOOKUP_SLICE_INDEX ? lookupSlice : slices.get(sliceIndex);
        }
    }
}
