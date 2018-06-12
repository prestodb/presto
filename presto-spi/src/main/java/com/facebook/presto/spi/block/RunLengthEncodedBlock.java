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

import java.util.function.BiConsumer;

import static com.facebook.presto.spi.block.BlockUtil.checkArrayRange;
import static com.facebook.presto.spi.block.BlockUtil.checkValidPosition;
import static com.facebook.presto.spi.block.BlockUtil.checkValidRegion;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RunLengthEncodedBlock
        implements Block
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(RunLengthEncodedBlock.class).instanceSize();

    public static Block create(Type type, Object value, int positionCount)
    {
        Block block = Utils.nativeValueToBlock(type, value);
        if (block instanceof RunLengthEncodedBlock) {
            block = ((RunLengthEncodedBlock) block).getValue();
        }
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

        if (value instanceof RunLengthEncodedBlock) {
            this.value = ((RunLengthEncodedBlock) value).getValue();
        }
        else {
            this.value = value;
        }

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }

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
    public long getSizeInBytes()
    {
        return value.getSizeInBytes();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + value.getRetainedSizeInBytes();
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(value, value.getRetainedSizeInBytes());
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public String getEncodingName()
    {
        return RunLengthBlockEncoding.NAME;
    }

    @Override
    public Block getPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);
        for (int i = offset; i < offset + length; i++) {
            checkValidPosition(positions[i], positionCount);
        }
        return new RunLengthEncodedBlock(value, length);
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);
        for (int i = offset; i < offset + length; i++) {
            checkValidPosition(positions[i], positionCount);
        }
        return new RunLengthEncodedBlock(value.copyRegion(0, 1), length);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(positionCount, positionOffset, length);
        return new RunLengthEncodedBlock(value, length);
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return value.getSizeInBytes();
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        checkValidRegion(positionCount, positionOffset, length);
        return new RunLengthEncodedBlock(value.copyRegion(0, 1), length);
    }

    @Override
    public int getSliceLength(int position)
    {
        checkReadablePosition(position);
        return value.getSliceLength(0);
    }

    @Override
    public byte getByte(int position, int offset)
    {
        checkReadablePosition(position);
        return value.getByte(0, offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        checkReadablePosition(position);
        return value.getShort(0, offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        checkReadablePosition(position);
        return value.getInt(0, offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        checkReadablePosition(position);
        return value.getLong(0, offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        checkReadablePosition(position);
        return value.getSlice(0, offset, length);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        checkReadablePosition(position);
        return value.getObject(0, clazz);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        checkReadablePosition(position);
        return value.bytesEqual(0, offset, otherSlice, otherOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        checkReadablePosition(position);
        return value.bytesCompare(0, offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        value.writeBytesTo(0, offset, length, blockBuilder);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        value.writePositionTo(0, blockBuilder);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        checkReadablePosition(position);
        return value.equals(0, offset, otherBlock, otherPosition, otherOffset, length);
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        checkReadablePosition(position);
        return value.hash(0, offset, length);
    }

    @Override
    public int compareTo(int leftPosition, int leftOffset, int leftLength, Block rightBlock, int rightPosition, int rightOffset, int rightLength)
    {
        checkReadablePosition(leftPosition);
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
    public Block getLoadedBlock()
    {
        Block loadedValueBlock = value.getLoadedBlock();

        if (loadedValueBlock == value) {
            return this;
        }
        return new RunLengthEncodedBlock(loadedValueBlock, positionCount);
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= positionCount) {
            throw new IllegalArgumentException("position is not valid");
        }
    }
}
