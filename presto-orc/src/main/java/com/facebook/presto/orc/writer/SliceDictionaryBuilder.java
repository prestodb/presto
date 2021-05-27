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
package com.facebook.presto.orc.writer;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.VariableWidthBlockBuilder;
import com.facebook.presto.orc.array.IntBigArray;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.common.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static com.google.common.base.Preconditions.checkArgument;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

// TODO this class is not memory efficient.  We can bypass all of the Presto type and block code
// since we are only interested in a hash of byte arrays.
public class SliceDictionaryBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SliceDictionaryBuilder.class).instanceSize();
    private static final float FILL_RATIO = 0.75f;
    private static final int EMPTY_SLOT = -1;
    private static final int EXPECTED_BYTES_PER_ENTRY = 32;

    private final IntBigArray blockPositionByHash = new IntBigArray();
    private VariableWidthBlockBuilder elementBlock;

    private int maxFill;
    private int hashMask;

    public SliceDictionaryBuilder(int expectedSize)
    {
        checkArgument(expectedSize >= 0, "expectedSize must not be negative");

        // todo we can do better
        int expectedEntries = min(expectedSize, DEFAULT_MAX_PAGE_SIZE_IN_BYTES / EXPECTED_BYTES_PER_ENTRY);
        // it is guaranteed expectedEntries * EXPECTED_BYTES_PER_ENTRY will not overflow
        this.elementBlock = new VariableWidthBlockBuilder(
                null,
                expectedEntries,
                expectedEntries * EXPECTED_BYTES_PER_ENTRY);

        int hashSize = arraySize(expectedSize, FILL_RATIO);
        this.maxFill = calculateMaxFill(hashSize);
        this.hashMask = hashSize - 1;

        blockPositionByHash.ensureCapacity(hashSize);
        blockPositionByHash.fill(EMPTY_SLOT);
    }

    public long getSizeInBytes()
    {
        return elementBlock.getSizeInBytes();
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + elementBlock.getRetainedSizeInBytes() + blockPositionByHash.sizeOf();
    }

    public int compareIndex(int left, int right)
    {
        return elementBlock.compareTo(
                left,
                0,
                elementBlock.getSliceLength(left),
                elementBlock,
                right,
                0,
                elementBlock.getSliceLength(right));
    }

    public int getSliceLength(int position)
    {
        return elementBlock.getSliceLength(position);
    }

    public Slice getSlice(int position, int length)
    {
        return elementBlock.getSlice(position, 0, length);
    }

    public Slice getRawSlice(int position)
    {
        return elementBlock.getRawSlice(position);
    }

    public int getRawSliceOffset(int position)
    {
        return elementBlock.getPositionOffset(position);
    }

    public void clear()
    {
        blockPositionByHash.fill(EMPTY_SLOT);
        elementBlock = (VariableWidthBlockBuilder) elementBlock.newBlockBuilderLike(null);
    }

    public int putIfAbsent(Block block, int position)
    {
        requireNonNull(block, "block must not be null");
        int blockPosition;
        long hashPosition = getHashPositionOfElement(block, position);
        if (blockPositionByHash.get(hashPosition) != EMPTY_SLOT) {
            blockPosition = blockPositionByHash.get(hashPosition);
        }
        else {
            blockPosition = addNewElement(hashPosition, block, position);
        }
        return blockPosition;
    }

    public int getEntryCount()
    {
        return elementBlock.getPositionCount();
    }

    /**
     * Get slot position of element at {@code position} of {@code block}
     */
    private long getHashPositionOfElement(Block block, int position)
    {
        checkArgument(!block.isNull(position), "position is null");
        int length = block.getSliceLength(position);
        long hashPosition = getMaskedHash(block.hash(position, 0, length));
        while (true) {
            int blockPosition = blockPositionByHash.get(hashPosition);
            if (blockPosition == EMPTY_SLOT) {
                // Doesn't have this element
                return hashPosition;
            }
            else if (elementBlock.getSliceLength(blockPosition) == length && block.equals(position, 0, elementBlock, blockPosition, 0, length)) {
                // Already has this element
                return hashPosition;
            }

            hashPosition = getMaskedHash(hashPosition + 1);
        }
    }

    private long getRehashPositionOfElement(Block block, int position)
    {
        checkArgument(!block.isNull(position), "position is null");
        int length = block.getSliceLength(position);
        long hashPosition = getMaskedHash(block.hash(position, 0, length));
        while (blockPositionByHash.get(hashPosition) != EMPTY_SLOT) {
            // in Re-hash there is no collision and continue to search until an empty spot is found.
            hashPosition = getMaskedHash(hashPosition + 1);
        }
        return hashPosition;
    }

    private int addNewElement(long hashPosition, Block block, int position)
    {
        checkArgument(!block.isNull(position), "position is null");
        block.writeBytesTo(position, 0, block.getSliceLength(position), elementBlock);
        elementBlock.closeEntry();

        int newElementPositionInBlock = elementBlock.getPositionCount() - 1;
        blockPositionByHash.set(hashPosition, newElementPositionInBlock);

        // increase capacity, if necessary
        if (elementBlock.getPositionCount() >= maxFill) {
            rehash(maxFill * 2);
        }

        return newElementPositionInBlock;
    }

    private void rehash(int size)
    {
        int newHashSize = arraySize(size + 1, FILL_RATIO);
        hashMask = newHashSize - 1;
        maxFill = calculateMaxFill(newHashSize);
        blockPositionByHash.ensureCapacity(newHashSize);
        blockPositionByHash.fill(EMPTY_SLOT);

        for (int blockPosition = 0; blockPosition < elementBlock.getPositionCount(); blockPosition++) {
            blockPositionByHash.set(getRehashPositionOfElement(elementBlock, blockPosition), blockPosition);
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
