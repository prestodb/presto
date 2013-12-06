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
package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
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
import static io.airlift.units.DataSize.Unit.BYTE;

public class ChannelSet
{
    private final SliceHashStrategy strategy;
    private final AddressValueSet addressValueSet;
    private final boolean containsNull;

    public ChannelSet(ChannelSet channelSet)
    {
        checkNotNull(channelSet, "channelSet is null");
        this.strategy = new SliceHashStrategy(channelSet.strategy);
        this.addressValueSet = new AddressValueSet(channelSet.addressValueSet, strategy);
        this.containsNull = channelSet.containsNull;
    }

    private ChannelSet(SliceHashStrategy strategy, AddressValueSet addressValueSet, boolean containsNull)
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

    public int size()
    {
        return addressValueSet.size();
    }

    public DataSize getEstimatedSize()
    {
        return new DataSize(addressValueSet.getEstimatedSize().toBytes() + strategy.getEstimatedSize().toBytes(), BYTE);
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
            return new DataSize(sizeOf(this.key) + sizeOf(this.used), BYTE);
        }
    }

    public static class ChannelSetBuilder
    {
        private final SliceHashStrategy strategy;
        private final AddressValueSet addressValueSet;
        private final OperatorContext operatorContext;
        private final TupleInfo tupleInfo;

        private int currentBlockId;
        private boolean containsNull;
        private BlockBuilder blockBuilder;

        // Note: Supporting multi-field channel sets (e.g. tuples) is much more difficult because of null handling, and hence is not supported by this class.
        public ChannelSetBuilder(TupleInfo tupleInfo, int expectedPositions, OperatorContext operatorContext)
        {
            this.tupleInfo = checkNotNull(tupleInfo, "tupleInfo is null");
            checkArgument(expectedPositions >= 0, "expectedPositions must be greater than or equal to zero");
            this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");


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

        public ChannelSet build()
        {
            return new ChannelSet(strategy, addressValueSet, containsNull);
        }
    }
}
