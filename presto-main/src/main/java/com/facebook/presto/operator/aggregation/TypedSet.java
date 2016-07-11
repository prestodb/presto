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

import com.facebook.presto.array.IntBigArray;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import io.airlift.units.DataSize;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.ExceededMemoryLimitException.exceededLocalLimit;
import static com.facebook.presto.type.TypeUtils.hashPosition;
import static com.facebook.presto.type.TypeUtils.positionEqualsPosition;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static java.util.Objects.requireNonNull;

public class TypedSet
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TypedSet.class).instanceSize();
    private static final float FILL_RATIO = 0.75f;
    private static final long FOUR_MEGABYTES = new DataSize(4, MEGABYTE).toBytes();

    private final Type elementType;
    private final IntBigArray blockPositionByHash = new IntBigArray();
    private final BlockBuilder elementBlock;

    private int maxFill;
    private int hashMask;
    private static final int EMPTY_SLOT = -1;

    private boolean containsNullElement;

    public TypedSet(Type elementType, int expectedSize)
    {
        checkArgument(expectedSize >= 0, "expectedSize must not be negative");
        this.elementType = requireNonNull(elementType, "elementType must not be null");
        this.elementBlock = elementType.createBlockBuilder(new BlockBuilderStatus(), expectedSize);

        int hashSize = arraySize(expectedSize, FILL_RATIO);
        this.maxFill = calculateMaxFill(hashSize);
        this.hashMask = hashSize - 1;

        blockPositionByHash.ensureCapacity(hashSize);
        for (int i = 0; i < hashSize; i++) {
            blockPositionByHash.set(i, EMPTY_SLOT);
        }

        this.containsNullElement = false;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + elementBlock.getRetainedSizeInBytes() + blockPositionByHash.sizeOf();
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
            long hashPosition = getHashPositionOfElement(block, position);
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
    private long getHashPositionOfElement(Block block, int position)
    {
        long hashPosition = getMaskedHash(hashPosition(elementType, block, position));
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

    private void addNewElement(long hashPosition, Block block, int position)
    {
        elementType.appendTo(block, position, elementBlock);
        if (elementBlock.getSizeInBytes() > FOUR_MEGABYTES) {
            throw exceededLocalLimit(new DataSize(4, MEGABYTE));
        }
        blockPositionByHash.set(hashPosition, elementBlock.getPositionCount() - 1);

        // increase capacity, if necessary
        if (elementBlock.getPositionCount() >= maxFill) {
            rehash(maxFill * 2);
        }
    }

    private void rehash(int size)
    {
        int newHashSize = arraySize(size + 1, FILL_RATIO);
        hashMask = newHashSize - 1;
        maxFill = calculateMaxFill(newHashSize);
        blockPositionByHash.ensureCapacity(newHashSize);
        for (int i = 0; i < newHashSize; i++) {
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
        int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
        if (maxFill == hashSize) {
            maxFill--;
        }
        return maxFill;
    }

    private long getMaskedHash(long rawHash)
    {
        return rawHash & hashMask;
    }
}
