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

import static com.facebook.presto.operator.SliceHashStrategy.LOOKUP_SLICE_INDEX;
import static com.facebook.presto.operator.SyntheticAddress.encodeSyntheticAddress;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;

public class ChannelSet
{
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

        TupleInfo tupleInfo = source.getTupleInfos().get(setChannel);
        checkArgument(tupleInfo.getFieldCount() == 1, "ChannelSet only supports single field set building channels");
        // Supporting multi-field channel sets (e.g. tuples) is much more difficult because of null handling, and hence is not supported by this class.

        // Construct the set from the source
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

                    // Record whether we have seen a null
                    containsNull |= sourceCursor.isNull(0); // There should only be one field in this channel!

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
}
