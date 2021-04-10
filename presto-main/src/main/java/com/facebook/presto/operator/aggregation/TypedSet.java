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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.openjdk.jol.info.ClassLayout;

import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_FUNCTION_MEMORY_LIMIT;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static com.facebook.presto.type.TypeUtils.hashPosition;
import static com.facebook.presto.type.TypeUtils.positionEqualsPosition;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TypedSet
{
    @VisibleForTesting
    public static final DataSize MAX_FUNCTION_MEMORY = new DataSize(4, MEGABYTE);

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TypedSet.class).instanceSize();
    private static final int INT_ARRAY_LIST_INSTANCE_SIZE = ClassLayout.parseClass(IntArrayList.class).instanceSize();
    private static final float FILL_RATIO = 0.75f;

    private final Type elementType;
    private final IntArrayList blockPositionByHash;
    private final BlockBuilder elementBlock;
    private final String functionName;
    private final long maxBlockMemoryInBytes;

    private int initialElementBlockOffset;
    private long initialElementBlockSizeInBytes;
    // size is the number of elements added to the TypedSet (including null).
    // It equals to elementBlock.size() - initialElementBlockOffset
    private int size;
    private int hashCapacity;
    private int maxFill;
    private int hashMask;
    private static final int EMPTY_SLOT = -1;

    private boolean containsNullElement;

    public TypedSet(Type elementType, int expectedSize, String functionName)
    {
        this(elementType, elementType.createBlockBuilder(null, expectedSize), expectedSize, functionName);
    }

    public TypedSet(Type elementType, BlockBuilder blockBuilder, int expectedSize, String functionName)
    {
        this(elementType, blockBuilder, expectedSize, functionName, Optional.of(MAX_FUNCTION_MEMORY));
    }

    public TypedSet(Type elementType, BlockBuilder blockBuilder, int expectedSize, String functionName, Optional<DataSize> maxBlockMemory)
    {
        checkArgument(expectedSize >= 0, "expectedSize must not be negative");
        this.elementType = requireNonNull(elementType, "elementType must not be null");
        this.elementBlock = requireNonNull(blockBuilder, "blockBuilder must not be null");
        this.functionName = functionName;
        this.maxBlockMemoryInBytes = requireNonNull(maxBlockMemory, "maxBlockMemory must not be null").map(DataSize::toBytes).orElse(Long.MAX_VALUE);

        initialElementBlockOffset = elementBlock.getPositionCount();
        initialElementBlockSizeInBytes = elementBlock.getSizeInBytes();

        this.size = 0;
        this.hashCapacity = arraySize(expectedSize, FILL_RATIO);
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

    public boolean add(Block block, int position)
    {
        requireNonNull(block, "block must not be null");
        checkArgument(position >= 0, "position must be >= 0");

        // containsNullElement flag is maintained so contains() method can have shortcut for null value
        if (block.isNull(position)) {
            containsNullElement = true;
        }

        int hashPosition = getHashPositionOfElement(block, position);
        if (blockPositionByHash.get(hashPosition) == EMPTY_SLOT) {
            addNewElement(hashPosition, block, position);
            return true;
        }

        return false;
    }

    public boolean addNonNull(Block block, int position)
    {
        requireNonNull(block, "block must not be null");
        checkArgument(position >= 0, "position must be >= 0");

        int hashPosition = getHashPositionOfElement(block, position);
        if (blockPositionByHash.get(hashPosition) == EMPTY_SLOT) {
            addNewElement(hashPosition, block, position);
            return true;
        }

        return false;
    }

    public int size()
    {
        return size;
    }

    public int positionOf(Block block, int position)
    {
        return blockPositionByHash.get(getHashPositionOfElement(block, position));
    }

    public BlockBuilder getBlockBuilder()
    {
        return elementBlock;
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
        if (elementBlock.getSizeInBytes() - initialElementBlockSizeInBytes > maxBlockMemoryInBytes) {
            throw new PrestoException(
                    EXCEEDED_FUNCTION_MEMORY_LIMIT,
                    format("The input to %s is too large. More than %s of memory is needed to hold the intermediate hash set.\n",
                            functionName,
                            MAX_FUNCTION_MEMORY));
        }
        blockPositionByHash.set(hashPosition, elementBlock.getPositionCount() - 1);

        // increase capacity, if necessary
        size++;
        if (size >= maxFill) {
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

        for (int blockPosition = initialElementBlockOffset; blockPosition < elementBlock.getPositionCount(); blockPosition++) {
            blockPositionByHash.set(getHashPositionOfElement(elementBlock, blockPosition), blockPosition);
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
