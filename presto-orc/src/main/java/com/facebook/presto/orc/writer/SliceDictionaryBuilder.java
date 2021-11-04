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

    private final IntBigArray slicePositionByHash = new IntBigArray();
    private final SegmentedSliceBlockBuilder segmentedSliceBuilder;

    private int maxFill;
    private int hashMask;

    public SliceDictionaryBuilder(int expectedSize)
    {
        checkArgument(expectedSize >= 0, "expectedSize must not be negative");

        // todo we can do better
        int expectedEntries = min(expectedSize, DEFAULT_MAX_PAGE_SIZE_IN_BYTES / EXPECTED_BYTES_PER_ENTRY);
        // it is guaranteed expectedEntries * EXPECTED_BYTES_PER_ENTRY will not overflow
        this.segmentedSliceBuilder = new SegmentedSliceBlockBuilder(
                expectedEntries,
                expectedEntries * EXPECTED_BYTES_PER_ENTRY);

        int hashSize = arraySize(expectedSize, FILL_RATIO);
        this.maxFill = calculateMaxFill(hashSize);
        this.hashMask = hashSize - 1;

        slicePositionByHash.ensureCapacity(hashSize);
        slicePositionByHash.fill(EMPTY_SLOT);
    }

    public long getSizeInBytes()
    {
        return segmentedSliceBuilder.getSizeInBytes();
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + segmentedSliceBuilder.getRetainedSizeInBytes() + slicePositionByHash.sizeOf();
    }

    public int compareIndex(int left, int right)
    {
        return segmentedSliceBuilder.compareTo(left, right);
    }

    public int getSliceLength(int position)
    {
        return segmentedSliceBuilder.getSliceLength(position);
    }

    public Slice getSlice(int position, int length)
    {
        return segmentedSliceBuilder.getSlice(position, 0, length);
    }

    public Slice getRawSlice(int position)
    {
        return segmentedSliceBuilder.getRawSlice(position);
    }

    public int getRawSliceOffset(int position)
    {
        return segmentedSliceBuilder.getPositionOffset(position);
    }

    public void clear()
    {
        slicePositionByHash.fill(EMPTY_SLOT);
        segmentedSliceBuilder.reset();
    }

    public int putIfAbsent(Block block, int position)
    {
        requireNonNull(block, "block must not be null");
        int slicePosition;
        long hashPosition = getHashPositionOfElement(block, position);
        if (slicePositionByHash.get(hashPosition) != EMPTY_SLOT) {
            slicePosition = slicePositionByHash.get(hashPosition);
        }
        else {
            slicePosition = addNewElement(hashPosition, block, position);
        }
        return slicePosition;
    }

    public int getEntryCount()
    {
        return segmentedSliceBuilder.getPositionCount();
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
            int slicePosition = this.slicePositionByHash.get(hashPosition);
            if (slicePosition == EMPTY_SLOT) {
                // Doesn't have this element
                return hashPosition;
            }
            if (segmentedSliceBuilder.equals(slicePosition, block, position, length)) {
                // Already has this element
                return hashPosition;
            }

            hashPosition = getMaskedHash(hashPosition + 1);
        }
    }

    private long getRehashPositionOfElement(int position)
    {
        long hashPosition = getMaskedHash(segmentedSliceBuilder.hash(position));
        while (slicePositionByHash.get(hashPosition) != EMPTY_SLOT) {
            // in Re-hash there is no collision and continue to search until an empty spot is found.
            hashPosition = getMaskedHash(hashPosition + 1);
        }
        return hashPosition;
    }

    private int addNewElement(long hashPosition, Block block, int position)
    {
        checkArgument(!block.isNull(position), "position is null");
        block.writeBytesTo(position, 0, block.getSliceLength(position), segmentedSliceBuilder);
        segmentedSliceBuilder.closeEntry();

        int newElementPositionInBlock = segmentedSliceBuilder.getPositionCount() - 1;
        slicePositionByHash.set(hashPosition, newElementPositionInBlock);

        // increase capacity, if necessary
        if (segmentedSliceBuilder.getPositionCount() >= maxFill) {
            rehash(maxFill * 2);
        }

        return newElementPositionInBlock;
    }

    private void rehash(int size)
    {
        int newHashSize = arraySize(size + 1, FILL_RATIO);
        hashMask = newHashSize - 1;
        maxFill = calculateMaxFill(newHashSize);
        slicePositionByHash.ensureCapacity(newHashSize);
        slicePositionByHash.fill(EMPTY_SLOT);

        for (int slicePosition = 0; slicePosition < segmentedSliceBuilder.getPositionCount(); slicePosition++) {
            slicePositionByHash.set(getRehashPositionOfElement(slicePosition), slicePosition);
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
