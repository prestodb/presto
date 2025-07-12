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
package com.facebook.presto.common.block;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import java.util.OptionalInt;
import java.util.function.ObjLongConsumer;

import static com.facebook.presto.common.block.BlockUtil.checkArrayRange;
import static com.facebook.presto.common.block.DictionaryId.randomDictionaryId;

/**
 * A block packs positionCount values into a chunk of memory. How the values are packed,
 * whether compression is used, endianness, and other implementation details are up to the subclasses.
 * However, for purposes of API, you can think of a Block as a sequence of zero-indexed values that
 * can be read by calling the getter methods in this interface. For instance,
 * you can read positionCount bytes by calling
 * block.getByte(0), block.getByte(1), ... block.getByte(positionCount - 1).
 * You can read positionCount longs by calling
 * block.getLong(0), block.getLong(1), ... block.getLong(positionCount - 1).
 * Of course the values returned might not make a lot of sense if
 * one type is written to a position and a different type is read.
 * Many subclasses will throw an UnsupportedOperationException if you try to read the
 * wrong type from a block. It's generally up to the reader to know what types to expect
 * where in each block.
 */
public interface Block
        extends UncheckedBlock
{
    /**
     * Gets the length of the value at the {@code position}.
     * This method must be implemented if @{code getSlice} is implemented.
     */
    default int getSliceLength(int position)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Gets a byte in the value at {@code position}.
     *
     * @throws IllegalArgumentException if position is negative or greater than or equal to the positionCount
     */
    default byte getByte(int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Gets a short in the value at {@code position}.
     *
     * @throws IllegalArgumentException if position is negative or greater than or equal to the positionCount
     */
    default short getShort(int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Gets an int in the value at {@code position}.
     *
     * @throws IllegalArgumentException if position is negative or greater than or equal to the positionCount
     */
    default int getInt(int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Gets a long in the value at {@code position}.
     *
     * @throws IllegalArgumentException if position is negative or greater than or equal to the positionCount
     */
    default long getLong(int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Gets a long at {@code offset} in the value at {@code position}.
     */
    default long getLong(int position, int offset)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Gets a slice at {@code offset} in the value at {@code position}.
     */
    default Slice getSlice(int position, int offset, int length)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Gets a block in the value at {@code position}.
     *
     * @throws IllegalArgumentException if position is negative or greater than or equal to the positionCount
     */
    default Block getBlock(int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Is the byte sequence at {@code offset} in the value at {@code position} equal
     * to the byte sequence at {@code otherOffset} in {@code otherSlice}.
     * This method must be implemented if @{code getSlice} is implemented.
     */
    default boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Compares the byte sequences at {@code offset} in the value at {@code position}
     * to the byte sequence at {@code otherOffset} in {@code otherSlice}.
     * This method must be implemented if @{code getSlice} is implemented.
     */
    default int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Appends the byte sequences at {@code offset} in the value at {@code position}
     * to {@code blockBuilder}.
     * This method must be implemented if @{code getSlice} is implemented.
     */
    default void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Appends the byte sequences at {@code offset} in the value at {@code position}
     * to {@code sliceOutput}.
     * This method must be implemented if @{code getSlice} is implemented.
     */
    default void writeBytesTo(int position, int offset, int length, SliceOutput sliceOutput)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Appends the value at {@code position} to {@code blockBuilder} and closes the entry.
     */
    void writePositionTo(int position, BlockBuilder blockBuilder);

    /**
     * Appends the value at {@code position} to {@code output}.
     */
    void writePositionTo(int position, SliceOutput output);

    /**
     * Is the byte sequences at {@code offset} in the value at {@code position} equal
     * to the byte sequence at {@code otherOffset} in the value at {@code otherPosition}
     * in {@code otherBlock}.
     * This method must be implemented if @{code getSlice} is implemented.
     */
    default boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Calculates a 64-bit hash code of the byte sequence at {@code offset} in the
     * value at {@code position}.
     * This method must be implemented if @{code getSlice} is implemented.
     */
    default long hash(int position, int offset, int length)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Compares the byte sequences at {@code offset} in the value at {@code position}
     * to the byte sequence at {@code otherOffset} in the value at {@code otherPosition}
     * in {@code otherBlock}.
     * This method must be implemented if @{code getSlice} is implemented.
     */
    default int compareTo(int leftPosition, int leftOffset, int leftLength, Block rightBlock, int rightPosition, int rightOffset, int rightLength)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Gets the value at the specified position as a single element block. The
     * returned block should retain only the memory required for a single block.
     * <p>
     * This method is useful for operators that hold on to a single value without
     * holding on to the entire block.
     *
     * @throws IllegalArgumentException if this position is not valid
     */
    Block getSingleValueBlock(int position);

    /**
     * Returns the number of positions in this block.
     */
    int getPositionCount();

    /**
     * Returns the size of this block as if it was compacted, ignoring any over-allocations.
     * For example, in dictionary blocks, this only counts each dictionary entry once,
     * rather than each time a value is referenced.
     */
    long getSizeInBytes();

    /**
     * Returns the size of the block contents, regardless of internal representation.
     * The same logical data values should always have the same size, no matter
     * what block type is used or how they are represented within a specific block.
     *
     * This can differ substantially from {@link #getSizeInBytes} for certain block
     * types. For RLE, it will be {@code N} times larger. For dictionary, it will be
     * larger based on how many times dictionary entries are reused.
     */
    default long getLogicalSizeInBytes()
    {
        return getSizeInBytes();
    }

    /**
     * Returns the size of {@code block.getRegion(position, length)}.
     * The method can be expensive. Do not use it outside an implementation of Block.
     */
    long getRegionSizeInBytes(int position, int length);

    /**
     * Returns the size of {@code block.getRegion(position, length)}.
     * The method can be expensive. Do not use it outside an implementation of Block.
     */
    default long getRegionLogicalSizeInBytes(int position, int length)
    {
        return getRegionSizeInBytes(position, length);
    }

    /**
     * Returns the approximate logical size of {@code block.getRegion(position, length)}.
     * This method is faster than getRegionLogicalSizeInBytes().
     * For dictionary blocks, this counts the amortized flattened size of the included positions.
     * For example, for a DictionaryBlock with 5 ids [1, 1, 1, 1, 1] and a dictionary of
     * VariableWidthBlock with 3 elements of sizes [9, 5, 7], the result of
     * getApproximateRegionLogicalSizeInBytes(0, 5) would be (9 + 5 + 7) / 3 * 5 = 35,
     * while getRegionLogicalSizeInBytes(0, 5) would be 5 * 5 = 25.
     */
    default long getApproximateRegionLogicalSizeInBytes(int position, int length)
    {
        return getRegionSizeInBytes(position, length);
    }

    /**
     * Returns the number of bytes (in terms of {@link Block#getSizeInBytes()}) required per position
     * that this block contains, assuming that the number of bytes required is a known static quantity
     * and not dependent on any particular specific position. This allows for some complex block wrappings
     * to potentially avoid having to call {@link Block#getPositionsSizeInBytes(boolean[], int)}  which
     * would require computing the specific positions selected
     * @return The size in bytes, per position, if this block type does not require specific position information to compute its size
     */
    OptionalInt fixedSizeInBytesPerPosition();

    /**
     * Returns the size of all positions marked true in the positions array.
     * This is equivalent to multiple calls of {@code block.getRegionSizeInBytes(position, length)}
     * where you mark all positions for the regions first.
     */
    long getPositionsSizeInBytes(boolean[] positions, int usedPositionCount);

    /**
     * Returns the retained size of this block in memory, including over-allocations.
     * This method is called from the inner most execution loop and must be fast.
     */
    long getRetainedSizeInBytes();

    /**
     * Returns the estimated in memory data size for stats of position.
     * Do not use it for other purpose.
     */
    long getEstimatedDataSizeForStats(int position);

    /**
     * {@code consumer} visits each of the internal data container and accepts the size for it.
     * This method can be helpful in cases such as memory counting for internal data structure.
     * Also, the method should be non-recursive, only visit the elements at the top level,
     * and specifically should not call retainedBytesForEachPart on nested blocks
     * {@code consumer} should be called at least once with the current block and
     * must include the instance size of the current block
     */
    void retainedBytesForEachPart(ObjLongConsumer<Object> consumer);

    /**
     * Get the encoding for this block.
     */
    String getEncodingName();

    /**
     * Create a new block from the current block by keeping the same elements
     * only with respect to {@code positions} that starts at {@code offset} and has length of {@code length}.
     * May return a view over the data in this block or may return a copy
     */
    default Block getPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        return new DictionaryBlock(offset, length, this, positions, false, randomDictionaryId());
    }

    /**
     * Returns a block containing the specified positions.
     * Positions to copy are stored in a subarray within {@code positions} array
     * that starts at {@code offset} and has length of {@code length}.
     * All specified positions must be valid for this block.
     * <p>
     * The returned block must be a compact representation of the original block.
     */
    Block copyPositions(int[] positions, int offset, int length);

    /**
     * Returns a block starting at the specified position and extends for the
     * specified length.  The specified region must be entirely contained
     * within this block.
     * <p>
     * The region can be a view over this block.  If this block is released
     * the region block may also be released.  If the region block is released
     * this block may also be released.
     */
    Block getRegion(int positionOffset, int length);

    /**
     * Returns a block starting at the specified position and extends for the
     * specified length.  The specified region must be entirely contained
     * within this block.
     * <p>
     * The region returned must be a compact representation of the original block, unless their internal
     * representation will be exactly the same. This method is useful for
     * operators that hold on to a range of values without holding on to the
     * entire block.
     */
    Block copyRegion(int position, int length);

    /**
     * Is it possible the block may have a null value?  If false, the block cannot contain
     * a null, but if true, the block may or may not have a null.
     */
    default boolean mayHaveNull()
    {
        return true;
    }

    /**
     * Is the specified position null?
     *
     * @throws IllegalArgumentException if this position is not valid
     */
    boolean isNull(int position);

    /**
     * Returns a block that assures all data is in memory.
     * May return the same block if all block data is already in memory.
     * <p>
     * This allows streaming data sources to skip sections that are not
     * accessed in a query.
     */
    default Block getLoadedBlock()
    {
        return this;
    }

    /**
     * Returns a block that has an appended null at the end, no matter if the original block has null or not.
     * The original block won't be modified.
     */
    Block appendNull();

    /**
     * Returns the converted long value at {@code position} if the value at {@code position} can be converted to long.
     *
     * Difference between toLong() and getLong() is:
     * getLong() would only return value when the block is LongArrayBlock, otherwise it would throw exception.
     * toLong() would return value for compatible types: LongArrayBlock, IntArrayBlock, ByteArrayBlock and ShortArrayBlock.
     *
     * @throws UnsupportedOperationException if value at {@code position} is not able to be converted to long.
     * @throws IllegalArgumentException if position is negative or greater than or equal to the positionCount
     */
    default long toLong(int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    // TODO #20578: Check if the following methods are really necessary. In Presto there is no ValueBlock.
    //                          It seems that these two methods and the implementations in all classes that inherit from Block can be removed.

    /**
     * Returns the underlying value block underlying this block.
     */
    default Block getUnderlyingValueBlock()
    {
        return this;
    }

    /**
     * Returns the position in the underlying value block corresponding to the specified position in this block.
     */
    default int getUnderlyingValuePosition(int position)
    {
        return position;
    }
}
