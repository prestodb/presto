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
package com.facebook.presto.block.rle;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.google.common.base.Objects;
import io.airlift.slice.Slice;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;

public class RunLengthEncodedBlock
        implements Block
{
    private final Block value;
    private final int positionCount;

    public RunLengthEncodedBlock(Block value, int positionCount)
    {
        this.value = checkNotNull(value, "value is null");
        checkArgument(value.getPositionCount() == 1, "Expected value to contain a single position but has %s positions", value.getPositionCount());

        // value can not be a RunLengthEncodedBlock because this could cause stack overflow in some of the methods
        checkArgument(!(value instanceof RunLengthEncodedBlock), "Value can not be an instance of a %s", getClass().getName());

        checkArgument(positionCount >= 0, "positionCount is negative");
        this.positionCount = checkNotNull(positionCount, "positionCount is null");
    }

    public Block getValue()
    {
        return value;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public int getSizeInBytes()
    {
        return value.getSizeInBytes();
    }

    @Override
    public RunLengthBlockEncoding getEncoding()
    {
        return new RunLengthBlockEncoding(value.getEncoding());
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkPositionIndexes(positionOffset, positionOffset + length, positionCount);
        return new RunLengthEncodedBlock(value, length);
    }

    @Override
    public int getLength(int position)
    {
        return value.getLength(0);
    }

    @Override
    public byte getByte(int position, int offset)
    {
        return value.getByte(0, offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        return value.getShort(0, offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        return value.getInt(0, offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        return value.getLong(0, offset);
    }

    @Override
    public float getFloat(int position, int offset)
    {
        return value.getFloat(0, offset);
    }

    @Override
    public double getDouble(int position, int offset)
    {
        return value.getDouble(0, offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        return value.getSlice(0, offset, length);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        return value.bytesEqual(0, offset, otherSlice, otherOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        return value.bytesCompare(0, offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        value.writeBytesTo(0, offset, length, blockBuilder);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        return value.equals(0, offset, otherBlock, otherPosition, otherOffset, length);
    }

    @Override
    public int hash(int position, int offset, int length)
    {
        return value.hash(0, offset, length);
    }

    @Override
    public int compareTo(int leftPosition, int leftOffset, int leftLength, Block rightBlock, int rightPosition, int rightOffset, int rightLength)
    {
        return value.compareTo(0, leftOffset, leftLength, rightBlock, rightPosition, rightOffset, rightLength);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        return value;
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return value.isNull(0);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("value", value)
                .add("positionCount", positionCount)
                .toString();
    }

    @Override
    public void assureLoaded()
    {
        value.assureLoaded();
    }

    private void checkReadablePosition(int position)
    {
        checkArgument(position >= 0 && position < positionCount, "position is not valid");
    }
}
