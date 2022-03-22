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

import com.facebook.presto.common.block.AbstractVariableWidthBlock;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockBuilderStatus;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.XxHash64;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.function.BiConsumer;

import static com.facebook.presto.orc.writer.SegmentedSliceBlockBuilder.Segments.INITIAL_SEGMENTS;
import static com.facebook.presto.orc.writer.SegmentedSliceBlockBuilder.Segments.SEGMENT_SIZE;
import static com.facebook.presto.orc.writer.SegmentedSliceBlockBuilder.Segments.offset;
import static com.facebook.presto.orc.writer.SegmentedSliceBlockBuilder.Segments.segment;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.String.format;

/**
 * Custom Block Builder implementation for use with SliceDictionaryBuilder.
 * Instead of using one large contiguous Slice for storing the unique Strings
 * in String dictionary, this class uses Segmented Slices. The main advantage
 * of this class over VariableWidthBlockBuilder is memory. Non contiguous
 * memory is more likely to be available and hence reduce the chance of OOMs.
 * <p>
 * Why implement a block builder ?
 * SliceDictionaryBuilder takes in a Block and Position to write.
 * 1. It can create a slice for the position and write it. This does not
 * require a block builder. But temporary slice, produces lot of
 * short lived garbage.
 * 2. A block and position can be copied to BlockBuilder using the method
 * Block.writeBytesTo . But this requires implementing the BlockBuilder interface.
 * Most of the methods are going to be unused and left as Unsupported.
 * <p>
 * What's the difference between this class and VariableWidthBlockBuilder?
 * This class is different from VariableWidthBlockBuilder in the following ways
 * 1. It does not support nulls. (So null byte array and management is avoided).
 * 2. Instead of using one contiguous chunk for storing all the entries,
 * they are segmented.
 * <p>
 * How is it implemented ?
 * The Strings from 0 to SEGMENT_SIZE-1 are stored in the first segment.
 * The string from SEGMENT_SIZE to 2 * SEGMENT_SIZE -1 goes to the second.
 * Each segment has Slice(data is concatenated and stored in one slice)
 * and offsets to capture the start offset and length. New slices are appended
 * to the open segment. Once the segment is full the segment is
 * finalized and appended to the closed segments. A new open segment is
 * created for further appends.
 */
public class SegmentedSliceBlockBuilder
        extends AbstractVariableWidthBlock
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SegmentedSliceBlockBuilder.class).instanceSize();

    private final DynamicSliceOutput openSliceOutput;

    private int openSegmentIndex;
    private int openSegmentOffset;
    private int[][] offsets;
    private Slice[] closedSlices;
    private long closedSlicesRetainedSize;
    private long closedSlicesSizeInBytes;

    public SegmentedSliceBlockBuilder(int expectedEntries, int expectedBytes)
    {
        int initialSize = Math.max(INITIAL_SEGMENTS, segment(expectedEntries) + 1);
        offsets = new int[initialSize][];
        closedSlices = new Slice[initialSize];
        offsets[0] = new int[SEGMENT_SIZE + 1];
        openSliceOutput = new DynamicSliceOutput(expectedBytes);
    }

    public void reset()
    {
        openSliceOutput.reset();

        Arrays.fill(closedSlices, null);
        closedSlicesRetainedSize = 0;
        closedSlicesSizeInBytes = 0;

        // Fill the first offset array with 0, and free up the rest of the offsets array.
        Arrays.fill(offsets[0], 0);
        Arrays.fill(offsets, 1, offsets.length, null);
        openSegmentIndex = 0;
        openSegmentOffset = 0;
    }

    @Override
    public int getPositionOffset(int position)
    {
        return getOffset(position);
    }

    @Override
    public int getSliceLength(int position)
    {
        int offset = offset(position);
        int segment = segment(position);
        return offsets[segment][offset + 1] - offsets[segment][offset];
    }

    private Slice getSegmentRawSlice(int segment)
    {
        return segment == openSegmentIndex ? openSliceOutput.getUnderlyingSlice() : closedSlices[segment];
    }

    @Override
    public Slice getRawSlice(int position)
    {
        return getSegmentRawSlice(segment(position));
    }

    @Override
    public int getPositionCount()
    {
        return Segments.getPositions(openSegmentIndex, openSegmentOffset);
    }

    @Override
    public long getSizeInBytes()
    {
        long offsetsSizeInBytes = Integer.BYTES * (long) getPositionCount();
        return openSliceOutput.size() + offsetsSizeInBytes + closedSlicesSizeInBytes;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        throw new UnsupportedOperationException("getRegionSizeInBytes is not supported by SegmentedSliceBlockBuilder");
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        throw new UnsupportedOperationException("getPositionsSizeInBytes is not supported by SegmentedSliceBlockBuilder");
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long offsetsSize = sizeOf(offsets) + (openSegmentIndex + 1) * sizeOf(offsets[0]);
        long closedSlicesSize = sizeOf(closedSlices) + closedSlicesRetainedSize;
        return INSTANCE_SIZE + openSliceOutput.getRetainedSize() + offsetsSize + closedSlicesSize;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        throw new UnsupportedOperationException("retainedBytesForEachPart is not supported by SegmentedSliceBlockBuilder");
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        throw new UnsupportedOperationException("copyPositions is not supported by SegmentedSliceBlockBuilder");
    }

    @Override
    public BlockBuilder writeByte(int value)
    {
        throw new UnsupportedOperationException("writeByte is not supported by SegmentedSliceBlockBuilder");
    }

    @Override
    public BlockBuilder writeShort(int value)
    {
        throw new UnsupportedOperationException("writeShort is not supported by SegmentedSliceBlockBuilder");
    }

    @Override
    public BlockBuilder writeInt(int value)
    {
        throw new UnsupportedOperationException("writeInt is not supported by SegmentedSliceBlockBuilder");
    }

    @Override
    public BlockBuilder writeLong(long value)
    {
        throw new UnsupportedOperationException("writeLong is not supported by SegmentedSliceBlockBuilder");
    }

    @Override
    public BlockBuilder writeBytes(Slice source, int sourceIndex, int length)
    {
        if (openSegmentOffset == 0) {
            // Expand Segments if necessary.
            if (openSegmentIndex >= offsets.length) {
                int newCapacity = Math.max(openSegmentIndex + 1, (int) (offsets.length * 1.5));
                closedSlices = Arrays.copyOf(closedSlices, newCapacity);
                offsets = Arrays.copyOf(offsets, newCapacity);
            }

            if (offsets[openSegmentIndex] == null) {
                offsets[openSegmentIndex] = new int[SEGMENT_SIZE + 1];
            }
        }
        openSliceOutput.writeBytes(source, sourceIndex, length);
        return this;
    }

    @Override
    public BlockBuilder closeEntry()
    {
        openSegmentOffset++;
        offsets[openSegmentIndex][openSegmentOffset] = openSliceOutput.size();
        if (openSegmentOffset == SEGMENT_SIZE) {
            // Copy the content from the openSlice and append it to the closedSlices.
            // Note: openSlice will be reused for next segment, so a copy is required.
            Slice slice = openSliceOutput.copySlice();
            closedSlices[openSegmentIndex] = slice;
            closedSlicesSizeInBytes += slice.length();
            closedSlicesRetainedSize += slice.getRetainedSize();

            // Prepare the open slice for next write.
            openSliceOutput.reset();
            openSegmentOffset = 0;
            openSegmentIndex++;
        }
        return this;
    }

    @Override
    public BlockBuilder appendNull()
    {
        throw new UnsupportedOperationException("appendNull is not supported by SegmentedSliceBlockBuilder");
    }

    @Override
    public BlockBuilder readPositionFrom(SliceInput input)
    {
        throw new UnsupportedOperationException("readPositionFrom is not supported by SegmentedSliceBlockBuilder");
    }

    @Override
    public boolean mayHaveNull()
    {
        return false;
    }

    @Override
    protected boolean isEntryNull(int position)
    {
        return false;
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        throw new UnsupportedOperationException("getRegion is not supported by SegmentedSliceBlockBuilder");
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        throw new UnsupportedOperationException("copyRegion is not supported by SegmentedSliceBlockBuilder");
    }

    @Override
    public Block build()
    {
        // No implementation of Segmented Slice based block exists. There is also no use for this yet.
        throw new UnsupportedOperationException("build is not supported by SegmentedSliceBlockBuilder");
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus)
    {
        return newBlockBuilderLike(blockBuilderStatus, getPositionCount());
    }

    @Override
    public BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        if (blockBuilderStatus != null) {
            throw new UnsupportedOperationException("blockBuilderStatus is not supported by SegmentedSliceBlockBuilder");
        }
        return new SegmentedSliceBlockBuilder(expectedEntries, openSliceOutput.getUnderlyingSlice().length());
    }

    private int getOffset(int position)
    {
        int offset = offset(position);
        int segment = segment(position);
        return offsets[segment][offset];
    }

    @Override
    public String toString()
    {
        return format("SegmentedSliceBlockBuilder(%d){positionCount=%d,size=%d}", hashCode(), getPositionCount(), openSliceOutput.size());
    }

    @Override
    public boolean isNullUnchecked(int internalPosition)
    {
        return false;
    }

    @Override
    public int getOffsetBase()
    {
        return 0;
    }

    public boolean equals(int position, Block block, int blockPosition, int blockLength)
    {
        int segment = segment(position);
        int segmentOffset = offset(position);

        int offset = offsets[segment][segmentOffset];
        int length = offsets[segment][segmentOffset + 1] - offset;
        return blockLength == length && block.bytesEqual(blockPosition, 0, getSegmentRawSlice(segment), offset, length);
    }

    public int compareTo(int left, int right)
    {
        int leftSegment = segment(left);
        int leftSegmentOffset = offset(left);

        int rightSegment = segment(right);
        int rightSegmentOffset = offset(right);

        Slice leftRawSlice = getSegmentRawSlice(leftSegment);
        int leftOffset = offsets[leftSegment][leftSegmentOffset];
        int leftLen = offsets[leftSegment][leftSegmentOffset + 1] - leftOffset;

        Slice rightRawSlice = getSegmentRawSlice(rightSegment);
        int rightOffset = offsets[rightSegment][rightSegmentOffset];
        int rightLen = offsets[rightSegment][rightSegmentOffset + 1] - rightOffset;

        return leftRawSlice.compareTo(leftOffset, leftLen, rightRawSlice, rightOffset, rightLen);
    }

    public long hash(int position)
    {
        int segment = segment(position);
        int segmentOffset = offset(position);

        int offset = offsets[segment][segmentOffset];
        int length = offsets[segment][segmentOffset + 1] - offset;
        // There are several methods which computes hash, Block.hash, BlockBuilder.hash and
        // Slice.hash. There is an expectations that all these methods return the same
        // hash value. For insertion, block.hash is called, but rehash relies on BlockBuilder.hash
        // So changing the hashing algorithm is hard. If a block implements different hashing
        // algorithm, it is going to produce incorrect results after rehashing.
        return XxHash64.hash(getSegmentRawSlice(segment), offset, length);
    }

    @VisibleForTesting
    int getOpenSegmentIndex()
    {
        return openSegmentIndex;
    }

    // This class is copied from com.facebook.presto.orc.array.BigArrays and the
    // Sizes and Initial Segments are tuned for the SliceDictionaryBuilder use case.
    static class Segments
    {
        public static final int INITIAL_SEGMENTS = 64;

        public static final int SEGMENT_SHIFT = 14;

        /**
         * Size of a single segment of a BigArray
         */
        public static final int SEGMENT_SIZE = 1 << SEGMENT_SHIFT;

        /**
         * The mask used to compute the offset associated to an index.
         */
        public static final int SEGMENT_MASK = SEGMENT_SIZE - 1;

        /**
         * Computes the segment associated with a given index.
         *
         * @param index an index into a big array.
         * @return the associated segment.
         */
        public static int segment(int index)
        {
            return index >>> SEGMENT_SHIFT;
        }

        /**
         * Computes the offset associated with a given index.
         *
         * @param index an index into a big array.
         * @return the associated offset (in the associated {@linkplain #segment(int) segment}).
         */
        public static int offset(int index)
        {
            return index & SEGMENT_MASK;
        }

        public static int getPositions(int segment, int offset)
        {
            return (segment << SEGMENT_SHIFT) + offset;
        }
    }
}
