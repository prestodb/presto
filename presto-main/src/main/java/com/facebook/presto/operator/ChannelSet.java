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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Murmur3;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.Arrays;

import static com.facebook.presto.operator.SyntheticAddress.decodePosition;
import static com.facebook.presto.operator.SyntheticAddress.decodeSliceIndex;
import static com.facebook.presto.operator.SyntheticAddress.encodeSyntheticAddress;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.units.DataSize.Unit.BYTE;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.maxFill;

// This implementation assumes arrays used in the hash are always a power of 2
public class ChannelSet
{
    private final Type type;
    private final ObjectArrayList<BlockBuilder> blocks;

    private final int mask;
    private final long[] key;
    private final boolean containsNull;

    private final DataSize estimatedSize;
    private final int size;

    public ChannelSet(Type type, ObjectArrayList<BlockBuilder> blocks, int mask, long[] key, boolean containsNull, DataSize estimatedSize, int size)
    {
        this.type = type;
        this.blocks = blocks;
        this.mask = mask;
        this.key = key;
        this.containsNull = containsNull;
        this.estimatedSize = estimatedSize;
        this.size = size;
    }

    public Type getType()
    {
        return type;
    }

    public DataSize getEstimatedSize()
    {
        return estimatedSize;
    }

    public int size()
    {
        return size;
    }

    public boolean containsNull()
    {
        return containsNull;
    }

    public boolean contains(int position, Block block)
    {
        int hashPosition = ((int) Murmur3.hash64(type.hash(block, position))) & mask;

        while (key[hashPosition] != -1) {
            long address = key[hashPosition];
            if (positionEqualsCurrentRow(decodeSliceIndex(address), decodePosition(address), position, block)) {
                // found an existing slot for this key
                return true;
            }
            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }
        return false;
    }

    private boolean positionEqualsCurrentRow(int sliceIndex, int slicePosition, int position, Block block)
    {
        return type.equalTo(blocks.get(sliceIndex), slicePosition, block, position);
    }

    public static class ChannelSetBuilder
    {
        private static final float FILL_RATIO = 0.75f;
        private final Type type;
        private final OperatorContext operatorContext;

        private final ObjectArrayList<BlockBuilder> blocks;

        private long completedBlocksMemorySize;

        private int maxFill;
        private int mask;
        private long[] key;
        private boolean containsNull;

        private int positionCount;

        public ChannelSetBuilder(Type type, int expectedPositions, OperatorContext operatorContext)
        {
            this.type = type;
            this.operatorContext = operatorContext;

            this.blocks = ObjectArrayList.wrap(new BlockBuilder[1024], 0);
            this.blocks.add(type.createBlockBuilder(new BlockBuilderStatus()));

            // reserve memory for the arrays
            int hashSize = arraySize(expectedPositions, FILL_RATIO);

            maxFill = maxFill(hashSize, FILL_RATIO);
            mask = hashSize - 1;
            key = new long[hashSize];
            Arrays.fill(key, -1);
        }

        public ChannelSet build()
        {
            return new ChannelSet(type, blocks, mask, key, containsNull, new DataSize(getEstimatedSize(), BYTE), positionCount);
        }

        public long getEstimatedSize()
        {
            return sizeOf(blocks.elements()) + completedBlocksMemorySize + blocks.get(blocks.size() - 1).getSizeInBytes() + sizeOf(key);
        }

        public int size()
        {
            return positionCount;
        }

        public void addBlock(Block block)
        {
            for (int position = 0; position < block.getPositionCount(); position++) {
                add(position, block);
            }

            if (operatorContext != null) {
                operatorContext.setMemoryReservation(getEstimatedSize());
            }
        }

        public void add(int position, Block block)
        {
            if (block.isNull(position)) {
                containsNull = true;
                return;
            }

            int hashPosition = ((int) Murmur3.hash64(type.hash(block, position))) & mask;

            // look for an empty slot or a slot containing this key
            while (key[hashPosition] != -1) {
                long address = key[hashPosition];
                if (positionEqualsPosition(decodeSliceIndex(address), decodePosition(address), position, block)) {
                    // value already present in set
                    return;
                }
                // increment position and mask to handle wrap around
                hashPosition = (hashPosition + 1) & mask;
            }

            addValue(hashPosition, position, block);
        }

        private void addValue(int hashPosition, int position, Block block)
        {
            // add the row to the open page
            int pageIndex = blocks.size() - 1;
            BlockBuilder blockBuilder = blocks.get(pageIndex);
            block.appendTo(position, blockBuilder);

            // record new value
            long address = encodeSyntheticAddress(pageIndex, blockBuilder.getPositionCount() - 1);
            key[hashPosition] = address;

            // create new block builder if this block is full
            if (blockBuilder.isFull()) {
                completedBlocksMemorySize += blockBuilder.getSizeInBytes();

                blockBuilder = type.createBlockBuilder(new BlockBuilderStatus());
                this.blocks.add(blockBuilder);
            }

            positionCount++;

            // increase capacity, if necessary
            if (positionCount >= maxFill) {
                rehash(maxFill * 2);
            }
        }

        private void rehash(int size)
        {
            int newSize = arraySize(size + 1, FILL_RATIO);

            int newMask = newSize - 1;
            long[] newKey = new long[newSize];
            Arrays.fill(newKey, -1);

            int oldIndex = 0;
            for (int position = 0; position < positionCount; position++) {
                // seek to the next used slot
                while (key[oldIndex] == -1) {
                    oldIndex++;
                }

                // get the address for this slot
                long address = key[oldIndex];

                // find an empty slot for the address
                int pos = ((int) Murmur3.hash64(hashPosition(address))) & newMask;
                while (newKey[pos] != -1) {
                    pos = (pos + 1) & newMask;
                }

                // record the mapping
                newKey[pos] = address;
                oldIndex++;
            }

            this.mask = newMask;
            this.maxFill = maxFill(newSize, FILL_RATIO);
            this.key = newKey;
        }

        private int hashPosition(long sliceAddress)
        {
            int sliceIndex = decodeSliceIndex(sliceAddress);
            int position = decodePosition(sliceAddress);
            return type.hash(blocks.get(sliceIndex), position);
        }

        private boolean positionEqualsPosition(int sliceIndex, int slicePosition, int position, Block block)
        {
            return type.equalTo(blocks.get(sliceIndex), slicePosition, block, position);
        }
    }
}
