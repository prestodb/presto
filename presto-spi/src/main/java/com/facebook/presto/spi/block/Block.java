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

public interface Block
{
    /**
     * Gets the length of the value at the {@code position}.
     */
    int getLength(int position);

    /**
     * Gets a byte at {@code offset} in the value at {@code position}.
     */
    byte getByte(int position, int offset);

    /**
     * Gets a little endian short at {@code offset} in the value at {@code position}.
     */
    short getShort(int position, int offset);

    /**
     * Gets a little endian int at {@code offset} in the value at {@code position}.
     */
    int getInt(int position, int offset);

    /**
     * Gets a little endian long at {@code offset} in the value at {@code position}.
     */
    long getLong(int position, int offset);

    /**
     * Gets a little endian float at {@code offset} in the value at {@code position}.
     */
    float getFloat(int position, int offset);

    /**
     * Gets a little endian double at {@code offset} in the value at {@code position}.
     */
    double getDouble(int position, int offset);

    /**
     * Gets a slice at {@code offset} in the value at {@code position}.
     */
    Slice getSlice(int position, int offset, int length);

    /**
     * Is the byte sequences at {@code offset} in the value at {@code position} equal
     * to the byte sequence at {@code otherOffset} in {@code otherSlice}.
     */
    boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length);

    /**
     * Compares the byte sequences at {@code offset} in the value at {@code position}
     * to the byte sequence at {@code otherOffset} in {@code otherSlice}.
     */
    int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength);

    /**
     * Appends the byte sequences at {@code offset} in the value at {@code position}
     * to {@code blockBuilder}.
     */
    void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder);

    /**
     * Is the byte sequences at {@code offset} in the value at {@code position} equal
     * to the byte sequence at {@code otherOffset} in the value at {@code otherPosition}
     * in {@code otherBlock}.
     */
    boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length);

    /**
     * Calculates the hash code the byte sequences at {@code offset} in the
     * value at {@code position}.
     */
    int hash(int position, int offset, int length);

    /**
     * Compares the byte sequences at {@code offset} in the value at {@code position}
     * to the byte sequence at {@code otherOffset} in the value at {@code otherPosition}
     * in {@code otherBlock}.
     */
    int compareTo(int leftPosition, int leftOffset, int leftLength, Block rightBlock, int rightPosition, int rightOffset, int rightLength);

    /**
     * Gets the value at the specified position as a single element block.
     *
     * @throws IllegalArgumentException if this position is not valid
     */
    Block getSingleValueBlock(int position);

    /**
     * Returns the number of positions in this block.
     */
    int getPositionCount();

    /**
     * Returns the size of this block in memory.
     */
    int getSizeInBytes();

    /**
     * Get the encoding for this block.
     */
    BlockEncoding getEncoding();

    /**
     * Returns a block starting at the specified position and extends for the
     * specified length.  The specified region must be entirely contained
     * within this block.
     */
    Block getRegion(int positionOffset, int length);

    /**
     * Is the specified position null?
     *
     * @throws IllegalArgumentException if this position is not valid
     */
    boolean isNull(int position);

    void assureLoaded();
}
