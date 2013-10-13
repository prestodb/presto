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
import com.facebook.presto.type.Type;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.longs.LongHash;
import it.unimi.dsi.fastutil.longs.LongOpenCustomHashSet;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.List;

import static com.facebook.presto.block.BlockBuilders.createBlockBuilder;
import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.operator.SyntheticAddress.encodeSyntheticAddress;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.units.DataSize.Unit.BYTE;

public class ChannelSet
{
    public static final long CURRENT_VALUE_ADDRESS = 0xFF_FF_FF_FF_FF_FF_FF_FFL;

    private final BlockBuilderHashStrategy strategy;
    private final AddressValueSet addressValueSet;
    private final boolean containsNull;
    private final DataSize estimatedSize;

    public ChannelSet(ChannelSet channelSet)
    {
        checkNotNull(channelSet, "channelSet is null");
        this.strategy = new BlockBuilderHashStrategy(channelSet.strategy);
        this.addressValueSet = new AddressValueSet(channelSet.addressValueSet, strategy);
        this.containsNull = channelSet.containsNull;
        this.estimatedSize = channelSet.estimatedSize;
    }

    private ChannelSet(BlockBuilderHashStrategy strategy, AddressValueSet addressValueSet, boolean containsNull, DataSize estimatedSize)
    {
        this.strategy = strategy;
        this.addressValueSet = addressValueSet;
        this.containsNull = containsNull;
        this.estimatedSize = estimatedSize;
    }

    public boolean containsNull()
    {
        return containsNull;
    }

    public void setCurrentValue(BlockCursor value)
    {
        strategy.setLookupValue(value);
    }

    public boolean containsCurrentValue()
    {
        return addressValueSet.contains(CURRENT_VALUE_ADDRESS);
    }

    public int size()
    {
        return addressValueSet.size();
    }

    public DataSize getEstimatedSize()
    {
        return estimatedSize;
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
        private final BlockBuilderHashStrategy strategy;
        private final AddressValueSet addressValueSet;
        private final OperatorContext operatorContext;
        private final Type type;
        private final ObjectArrayList<BlockBuilder> blocks;

        private int currentBlockId;
        private boolean containsNull;
        private BlockBuilder openBlockBuilder;
        private long blocksMemorySize;

        // Note: Supporting multi-channel sets (e.g. tuples) is much more difficult because of null handling, and hence is not supported by this class.
        public ChannelSetBuilder(Type type, int expectedPositions, OperatorContext operatorContext)
        {
            this.type = checkNotNull(type, "type is null");
            checkArgument(expectedPositions >= 0, "expectedPositions must be greater than or equal to zero");
            this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");

            // Construct the set from the source
            blocks = ObjectArrayList.wrap(new BlockBuilder[1024], 0);
            strategy = new BlockBuilderHashStrategy(blocks);
            addressValueSet = new AddressValueSet(expectedPositions, strategy);

            openBlockBuilder = createBlockBuilder(type);
            blocks.add(openBlockBuilder);
        }

        public void addBlock(Block sourceBlock)
        {
            BlockCursor sourceCursor = sourceBlock.cursor();
            strategy.setLookupValue(sourceCursor);

            while (sourceCursor.advanceNextPosition()) {
                // Record whether we have seen a null
                containsNull |= sourceCursor.isNull();

                if (!addressValueSet.contains(CURRENT_VALUE_ADDRESS)) {
                    if (openBlockBuilder.isFull()) {
                        // record the memory usage for the open block
                        blocksMemorySize += openBlockBuilder.getDataSize().toBytes();

                        // create a new block builder (there is no need to actually "build" the block)
                        openBlockBuilder = createBlockBuilder(type);
                        blocks.add(openBlockBuilder);
                        currentBlockId++;
                    }

                    int blockPosition = openBlockBuilder.getPositionCount();
                    sourceCursor.appendTo(openBlockBuilder);
                    addressValueSet.add(encodeSyntheticAddress(currentBlockId, blockPosition));
                }
            }

            operatorContext.setMemoryReservation(getEstimatedSize());
        }

        public long getEstimatedSize()
        {
            return blocksMemorySize;
        }

        public ChannelSet build()
        {
            DataSize estimatedSize = new DataSize(addressValueSet.getEstimatedSize().toBytes() + blocksMemorySize + openBlockBuilder.size(), BYTE);
            return new ChannelSet(strategy, addressValueSet, containsNull, estimatedSize);
        }
    }

    private static class BlockBuilderHashStrategy
            implements LongHash.Strategy
    {
        private final List<BlockBuilder> blocks;
        private BlockCursor lookupValue;

        public BlockBuilderHashStrategy(BlockBuilderHashStrategy strategy)
        {
            this(checkNotNull(strategy, "strategy is null").blocks);
        }

        private BlockBuilderHashStrategy(List<BlockBuilder> blocks)
        {
            checkNotNull(blocks, "blocks is null");
            this.blocks = blocks;
        }

        public void setLookupValue(BlockCursor lookupValue)
        {
            checkNotNull(lookupValue, "lookupValue is null");
            this.lookupValue = lookupValue;
        }

        @Override
        public int hashCode(long sliceAddress)
        {
            if (sliceAddress == CURRENT_VALUE_ADDRESS) {
                return hashCurrentRow();
            }
            else {
                return hashPosition(sliceAddress);
            }
        }

        private int hashPosition(long sliceAddress)
        {
            int sliceIndex = decodeSliceIndex(sliceAddress);
            int position = decodePosition(sliceAddress);
            return blocks.get(sliceIndex).hashCode(position);
        }

        private int hashCurrentRow()
        {
            return lookupValue.calculateHashCode();
        }

        @Override
        public boolean equals(long leftSliceAddress, long rightSliceAddress)
        {
            // current row always equals itself
            if (leftSliceAddress == CURRENT_VALUE_ADDRESS && rightSliceAddress == CURRENT_VALUE_ADDRESS) {
                return true;
            }

            // current row == position
            if (leftSliceAddress == CURRENT_VALUE_ADDRESS) {
                return positionEqualsCurrentRow(decodeSliceIndex(rightSliceAddress), decodePosition(rightSliceAddress));
            }

            // position == current row
            if (rightSliceAddress == CURRENT_VALUE_ADDRESS) {
                return positionEqualsCurrentRow(decodeSliceIndex(leftSliceAddress), decodePosition(leftSliceAddress));
            }

            // position == position
            return positionEqualsPosition(
                    decodeSliceIndex(leftSliceAddress), decodePosition(leftSliceAddress),
                    decodeSliceIndex(rightSliceAddress), decodePosition(rightSliceAddress));
        }

        private boolean positionEqualsCurrentRow(int sliceIndex, int position)
        {
            return blocks.get(sliceIndex).equals(position, lookupValue);
        }

        private boolean positionEqualsPosition(int leftSliceIndex, int leftPosition, int rightSliceIndex, int rightPosition)
        {
            return blocks.get(leftSliceIndex).equals(leftPosition, blocks.get(rightSliceIndex), rightPosition);
        }
    }
}
