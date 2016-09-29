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
package com.facebook.presto.spi.block;

import io.airlift.slice.Slice;

import java.util.List;

public interface Block
{
    /**
     * Gets the length of the value at the {@code position}.
     */
    int getLength(int position);

    /**
     * Gets a byte at {@code offset} in the value at {@code position}.
     */
    default byte getByte(int position, int offset)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Gets a little endian short at {@code offset} in the value at {@code position}.
     */
    default short getShort(int position, int offset)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Gets a little endian int at {@code offset} in the value at {@code position}.
     */
    default int getInt(int position, int offset)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Gets a little endian long at {@code offset} in the value at {@code position}.
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
     * Gets an object in the value at {@code position}.
     */
    default <T> T getObject(int position, Class<T> clazz)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Is the byte sequences at {@code offset} in the value at {@code position} equal
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
     * Appends the value at {@code position} to {@code blockBuilder}.
     */
    void writePositionTo(int position, BlockBuilder blockBuilder);

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
     * Calculates the hash code the byte sequences at {@code offset} in the
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
     * Gets the value at the specified position as a single element block.  The method
     * must copy the data into a new block.
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
     * Returns the logical size of this block in memory.
     */
    int getSizeInBytes();

    /**
     * Returns the retained size of this block in memory.
     * This method is called from the inner most execution loop and must be fast.
     */
    int getRetainedSizeInBytes();

    /**
     * Get the encoding for this block.
     */
    BlockEncoding getEncoding();

    /**
     * Returns a block containing the specified positions.
     * All specified positions must be valid for this block.
     * <p>
     * The returned block must be a compact representation of the original block.
     */
    Block copyPositions(List<Integer> positions);

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
     * Is the specified position null?
     *
     * @throws IllegalArgumentException if this position is not valid
     */
    boolean isNull(int position);

    /**
     * Assures that all data for the block is in memory.
     * <p>
     * This allows streaming data sources to skip sections that are not
     * accessed in a query.
     */
    default void assureLoaded() {}
}
