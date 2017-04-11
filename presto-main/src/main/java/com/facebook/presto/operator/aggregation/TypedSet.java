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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.ExceededMemoryLimitException.exceededLocalLimit;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static com.facebook.presto.type.TypeUtils.hashPosition;
import static com.facebook.presto.type.TypeUtils.positionEqualsPosition;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static java.util.Objects.requireNonNull;

public class TypedSet
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TypedSet.class).instanceSize();
    private static final int INT_ARRAY_LIST_INSTANCE_SIZE = ClassLayout.parseClass(IntArrayList.class).instanceSize();
    private static final float FILL_RATIO = 0.75f;
    private static final long FOUR_MEGABYTES = new DataSize(4, MEGABYTE).toBytes();

    private final Type elementType;
    private final IntArrayList blockPositionByHash;
    private final BlockBuilder elementBlock;

    private int hashCapacity;
    private int maxFill;
    private int hashMask;
    private static final int EMPTY_SLOT = -1;

    private boolean containsNullElement;

    public TypedSet(Type elementType, int expectedSize)
    {
        checkArgument(expectedSize >= 0, "expectedSize must not be negative");
        this.elementType = requireNonNull(elementType, "elementType must not be null");
        this.elementBlock = elementType.createBlockBuilder(new BlockBuilderStatus(), expectedSize);

        hashCapacity = arraySize(expectedSize, FILL_RATIO);
        this.maxFill = calculateMaxFill(hashCapacity);
        this.hashMask = hashCapacity - 1;

        blockPositionByHash = new IntArrayList(hashCapacity);
        blockPositionByHash.size(hashCapacity);
        for (int i = 0; i < hashCapacity; i++) {
            blockPositionByHash.set(i, EMPTY_SLOT);
        }

        this.containsNullElement = false;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + INT_ARRAY_LIST_INSTANCE_SIZE + elementBlock.getRetainedSizeInBytes() + blockPositionByHash.size() * Integer.BYTES;
    }

    public boolean contains(Block block, int position)
    {
        requireNonNull(block, "block must not be null");
        checkArgument(position >= 0, "position must be >= 0");

        if (block.isNull(position)) {
            return containsNullElement;
        }
        else {
            return blockPositionByHash.get(getHashPositionOfElement(block, position)) != EMPTY_SLOT;
        }
    }

    public void add(Block block, int position)
    {
        requireNonNull(block, "block must not be null");
        checkArgument(position >= 0, "position must be >= 0");

        if (block.isNull(position)) {
            containsNullElement = true;
        }
        else {
            int hashPosition = getHashPositionOfElement(block, position);
            if (blockPositionByHash.get(hashPosition) == EMPTY_SLOT) {
                addNewElement(hashPosition, block, position);
            }
        }
    }

    public int size()
    {
        return elementBlock.getPositionCount() + (containsNullElement ? 1 : 0);
    }

    public int positionOf(Block block, int position)
    {
        return blockPositionByHash.get(getHashPositionOfElement(block, position));
    }

    /**
     * Get slot position of element at {@code position} of {@code block}
     */
    private int getHashPositionOfElement(Block block, int position)
    {
        int hashPosition = getMaskedHash(hashPosition(elementType, block, position));
        while (true) {
            int blockPosition = blockPositionByHash.get(hashPosition);
            // Doesn't have this element
            if (blockPosition == EMPTY_SLOT) {
                return hashPosition;
            }
            // Already has this element
            else if (positionEqualsPosition(elementType, elementBlock, blockPosition, block, position)) {
                return hashPosition;
            }

            hashPosition = getMaskedHash(hashPosition + 1);
        }
    }

    private void addNewElement(int hashPosition, Block block, int position)
    {
        elementType.appendTo(block, position, elementBlock);
        if (elementBlock.getSizeInBytes() > FOUR_MEGABYTES) {
            throw exceededLocalLimit(new DataSize(4, MEGABYTE));
        }
        blockPositionByHash.set(hashPosition, elementBlock.getPositionCount() - 1);

        // increase capacity, if necessary
        if (elementBlock.getPositionCount() >= maxFill) {
            rehash();
        }
    }

    private void rehash()
    {
        long newCapacityLong = hashCapacity * 2L;
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new PrestoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = (int) newCapacityLong;

        hashCapacity = newCapacity;
        hashMask = newCapacity - 1;
        maxFill = calculateMaxFill(newCapacity);
        blockPositionByHash.size(newCapacity);
        for (int i = 0; i < newCapacity; i++) {
            blockPositionByHash.set(i, EMPTY_SLOT);
        }

        rehashBlock(elementBlock);
    }

    private void rehashBlock(Block block)
    {
        for (int blockPosition = 0; blockPosition < block.getPositionCount(); blockPosition++) {
            blockPositionByHash.set(getHashPositionOfElement(block, blockPosition), blockPosition);
        }
    }

    private static int calculateMaxFill(int hashSize)
    {
        checkArgument(hashSize > 0, "hashSize must be greater than 0");
        int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
        if (maxFill == hashSize) {
            maxFill--;
        }
        checkArgument(hashSize > maxFill, "hashSize must be larger than maxFill");
        return maxFill;
    }

    private int getMaskedHash(long rawHash)
    {
        return (int) (rawHash & hashMask);
    }
}
