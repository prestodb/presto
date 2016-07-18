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

import com.facebook.presto.spi.predicate.Utils;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static com.facebook.presto.spi.block.BlockUtil.checkValidPositions;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RunLengthEncodedBlock
        implements Block
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(RunLengthEncodedBlock.class).instanceSize();

    public static Block create(Type type, Object value, int positionCount)
    {
        Block block = Utils.nativeValueToBlock(type, value);
        return new RunLengthEncodedBlock(block, positionCount);
    }

    private final Block value;
    private final int positionCount;

    public RunLengthEncodedBlock(Block value, int positionCount)
    {
        requireNonNull(value, "value is null");
        if (value.getPositionCount() != 1) {
            throw new IllegalArgumentException(format("Expected value to contain a single position but has %s positions", value.getPositionCount()));
        }

        // value can not be a RunLengthEncodedBlock because this could cause stack overflow in some of the methods
        if (value instanceof RunLengthEncodedBlock) {
            throw new IllegalArgumentException(format("Value can not be an instance of a %s", getClass().getName()));
        }

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }

        this.value = value;
        this.positionCount = positionCount;
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
    public int getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + value.getRetainedSizeInBytes();
    }

    @Override
    public RunLengthBlockEncoding getEncoding()
    {
        return new RunLengthBlockEncoding(value.getEncoding());
    }

    @Override
    public Block copyPositions(List<Integer> positions)
    {
        checkValidPositions(positions, positionCount);
        return new RunLengthEncodedBlock(value.copyRegion(0, 1), positions.size());
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkPositionIndexes(positionOffset, length);
        return new RunLengthEncodedBlock(value, length);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        checkPositionIndexes(positionOffset, length);
        return new RunLengthEncodedBlock(value.copyRegion(0, 1), length);
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
    public Slice getSlice(int position, int offset, int length)
    {
        return value.getSlice(0, offset, length);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        return value.getObject(0, clazz);
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
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        value.writePositionTo(position, blockBuilder);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        return value.equals(0, offset, otherBlock, otherPosition, otherOffset, length);
    }

    @Override
    public long hash(int position, int offset, int length)
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
        StringBuilder sb = new StringBuilder(getClass().getSimpleName());
        sb.append("positionCount=").append(positionCount);
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public void assureLoaded()
    {
        value.assureLoaded();
    }

    private void checkPositionIndexes(int positionOffset, int length)
    {
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException("Invalid position " + positionOffset + " in block with " + positionCount + " positions");
        }
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= positionCount) {
            throw new IllegalArgumentException("position is not valid");
        }
    }
}
