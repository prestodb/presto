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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.TypeUtils;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.slice.SizeOf.sizeOf;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;

public class SimpleTypedSet implements TypedSet
{
    private static final int DEFAULT_EXPECTED_SIZE = 10;
    private static final float FILL_RATIO = 0.75f;

    private final Type elementType;
    private final BlockBuilder elementBuilder;

    private int maxFill;
    private int hashMask;
    private int[] blockPositionByHash;
    private static final int EMPTY_SLOT = -1;

    private boolean containsNullElement;

    public SimpleTypedSet(final Type elementType)
    {
        this(elementType, DEFAULT_EXPECTED_SIZE);
    }

    public SimpleTypedSet(final Type elementType, final int expectedSize)
    {
        checkArgument(expectedSize > 0, "expectedSize must > 0");
        this.elementType = checkNotNull(elementType, "elementType must not null");
        this.elementBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), expectedSize);

        final int hashSize = arraySize(expectedSize, FILL_RATIO);
        this.maxFill = calculateMaxFill(hashSize);
        this.hashMask = hashSize - 1;

        this.blockPositionByHash = new int[hashSize];
        Arrays.fill(blockPositionByHash, EMPTY_SLOT);

        this.containsNullElement = false;
    }

    @Override
    public Type getType()
    {
        return elementType;
    }

    @Override
    public long getEstimatedSize()
    {
        return elementBuilder.getSizeInBytes() + sizeOf(blockPositionByHash);
    }

    @Override
    public boolean contains(final Block block, final int position)
    {
        checkNotNull(block, "block should not be null");
        checkArgument(position >= 0, "position should >= 0");

        if (block.isNull(position)) {
            return containsNullElement;
        }
        else {
            return blockPositionByHash[getHashPositionOfElement(block, position)] != EMPTY_SLOT;
        }
    }

    @Override
    public void add(final Block block, final int position)
    {
        checkNotNull(block, "block should not be null");
        checkArgument(position >= 0, "position should >= 0");

        if (block.isNull(position)) {
            containsNullElement = true;
        }
        else {
            final int hashPosition = getHashPositionOfElement(block, position);
            if (blockPositionByHash[hashPosition] == EMPTY_SLOT) {
                addNewElement(hashPosition, block, position);
            }
        }
    }

    @Override
    public int size()
    {
        return elementBuilder.getPositionCount() + (containsNullElement ? 1 : 0);
    }

    /**
     * Get slot position of element at {@code position} of {@code block}
     */
    private int getHashPositionOfElement(final Block block, final int position)
    {
        int hashPosition = getHashUnderMask(TypeUtils.hashPosition(elementType, block, position));
        while (true) {
            final int blockPosition = blockPositionByHash[hashPosition];
            // Doesn't have this element
            if (blockPosition == EMPTY_SLOT) {
                return hashPosition;
            }
            // Already has this element
            else if (TypeUtils.positionEqualsPosition(elementType, elementBuilder, blockPosition, block, position)) {
                return hashPosition;
            }

            hashPosition = getHashUnderMask(hashPosition + 1);
        }
    }

    private void addNewElement(final int hashPosition, final Block block, final int position)
    {
        checkArgument(hashPosition >= 0, "position must >= 0");
        checkNotNull(block, "block must not be null");
        checkArgument(position >= 0, "position must >= 0");

        elementType.appendTo(block, position, elementBuilder);
        blockPositionByHash[hashPosition] = elementBuilder.getPositionCount() - 1;

        // increase capacity, if necessary
        if (elementBuilder.getPositionCount() >= maxFill) {
            rehash(maxFill * 2);
        }
    }

    private void rehash(final int size)
    {
        checkArgument(size > 0, "Size must > 0");

        final int newHashSize = arraySize(size + 1, FILL_RATIO);
        checkArgument(newHashSize > blockPositionByHash.length, "New hash size should > previous size");
        hashMask = newHashSize - 1;
        maxFill = calculateMaxFill(newHashSize);
        blockPositionByHash = new int[newHashSize];
        Arrays.fill(blockPositionByHash, EMPTY_SLOT);

        addBlock(elementBuilder);
    }

    private void addBlock(final Block block)
    {
        checkNotNull(block, "block must not be null");

        checkArgument(block.getPositionCount() < maxFill, "Elements number in block should less than maxFill");
        for (int blockPosition = 0; blockPosition < block.getPositionCount(); blockPosition++) {
            final int hashPosition = getHashPositionOfElement(block, blockPosition);
            if (blockPositionByHash[hashPosition] == EMPTY_SLOT) {
                blockPositionByHash[hashPosition] = blockPosition;
            }
        }
    }

    private static int calculateMaxFill(final int hashSize)
    {
        checkArgument(hashSize > 0, "hashSize must > 0");

        int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
        if (maxFill == hashSize) {
            maxFill--;
        }
        checkArgument(hashSize > maxFill, "hashSize must be larger than maxFill");
        return maxFill;
    }

    private int getHashUnderMask(final int rawHash)
    {
        return rawHash & hashMask;
    }
}
