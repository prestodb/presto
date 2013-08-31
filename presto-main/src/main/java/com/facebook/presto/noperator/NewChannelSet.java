package com.facebook.presto.noperator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.operator.SliceHashStrategy;
import com.facebook.presto.operator.SyntheticAddress;
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

public class NewChannelSet
{
    private final SliceHashStrategy strategy;
    private final AddressValueSet addressValueSet;
    private final boolean containsNull;

    public NewChannelSet(NewChannelSet channelSet)
    {
        checkNotNull(channelSet, "channelSet is null");
        this.strategy = new SliceHashStrategy(channelSet.strategy);
        this.addressValueSet = new AddressValueSet(channelSet.addressValueSet, strategy);
        this.containsNull = channelSet.containsNull;
    }

    private NewChannelSet(SliceHashStrategy strategy, AddressValueSet addressValueSet, boolean containsNull)
    {
        this.strategy = strategy;
        this.addressValueSet = addressValueSet;
        this.containsNull = containsNull;
    }

    public boolean containsNull()
    {
        return containsNull;
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

    public static class NewChannelSetBuilder
    {
        private final SliceHashStrategy strategy;
        private final AddressValueSet addressValueSet;
        private final OperatorContext operatorContext;
        private final TupleInfo tupleInfo;

        private int currentBlockId;
        private boolean containsNull;
        private BlockBuilder blockBuilder;

        public NewChannelSetBuilder(TupleInfo tupleInfo, int expectedPositions, OperatorContext operatorContext)
        {
            this.tupleInfo = checkNotNull(tupleInfo, "tupleInfo is null");
            checkArgument(expectedPositions >= 0, "expectedPositions must be greater than or equal to zero");
            this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");

            checkArgument(tupleInfo.getFieldCount() == 1, "ChannelSet only supports single field set building channels");
            // Supporting multi-field channel sets (e.g. tuples) is much more difficult because of null handling, and hence is not supported by this class.

            // Construct the set from the source
            strategy = new SliceHashStrategy(tupleInfo);
            addressValueSet = new AddressValueSet(expectedPositions, strategy);

            // allocate the first slice of the set
            Slice slice = Slices.allocate((int) BlockBuilder.DEFAULT_MAX_BLOCK_SIZE.toBytes());
            strategy.addSlice(slice);
            blockBuilder = new BlockBuilder(tupleInfo, slice.length(), slice.getOutput());
        }

        public void addBlock(Block sourceBlock)
        {
            operatorContext.setMemoryReservation(getEstimatedSize());

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
                        Slice slice = Slices.allocate(Math.max((int) BlockBuilder.DEFAULT_MAX_BLOCK_SIZE.toBytes(), length));
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

        public long getEstimatedSize()
        {
            return addressValueSet.getEstimatedSize().toBytes() + strategy.getEstimatedSize().toBytes();
        }

        public NewChannelSet build()
        {
            return new NewChannelSet(strategy, addressValueSet, containsNull);
        }
    }
}
