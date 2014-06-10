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

import com.facebook.presto.spi.type.FixedWidthType;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

public class FixedWidthBlockBuilder
        extends AbstractFixedWidthBlock
        implements BlockBuilder
{
    private final BlockBuilderStatus blockBuilderStatus;
    private final SliceOutput sliceOutput;
    private int positionCount;

    public FixedWidthBlockBuilder(FixedWidthType type, BlockBuilderStatus blockBuilderStatus)
    {
        super(type);

        this.blockBuilderStatus = blockBuilderStatus;
        this.sliceOutput = new DynamicSliceOutput(blockBuilderStatus.getMaxBlockSizeInBytes());
    }

    public FixedWidthBlockBuilder(FixedWidthType type, int positionCount)
    {
        super(type);

        Slice slice = Slices.allocate(entrySize * positionCount);

        this.blockBuilderStatus = new BlockBuilderStatus(slice.length(), slice.length());
        this.sliceOutput = slice.getOutput();
    }

    @Override
    protected Slice getRawSlice()
    {
        return sliceOutput.getUnderlyingSlice();
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public boolean isEmpty()
    {
        return positionCount == 0;
    }

    @Override
    public boolean isFull()
    {
        return blockBuilderStatus.isFull();
    }

    @Override
    public int getSizeInBytes()
    {
        return getRawSlice().length();
    }

    @Override
    public BlockBuilder appendBoolean(boolean value)
    {
        sliceOutput.writeByte(0);
        type.writeBoolean(sliceOutput, value);
        entryAdded();
        return this;
    }

    @Override
    public BlockBuilder appendLong(long value)
    {
        sliceOutput.writeByte(0);
        type.writeLong(sliceOutput, value);
        entryAdded();
        return this;
    }

    @Override
    public BlockBuilder appendDouble(double value)
    {
        sliceOutput.writeByte(0);
        type.writeDouble(sliceOutput, value);
        entryAdded();
        return this;
    }

    @Override
    public BlockBuilder appendSlice(Slice value)
    {
        return appendSlice(value, 0, value.length());
    }

    @Override
    public BlockBuilder appendSlice(Slice value, int offset, int length)
    {
        if (length != type.getFixedSize()) {
            throw new IllegalArgumentException("length must be " + type.getFixedSize() + " but is " + length);
        }

        sliceOutput.writeByte(0);

        type.writeSlice(sliceOutput, value, offset);

        entryAdded();

        return this;
    }

    @Override
    public BlockBuilder appendNull()
    {
        sliceOutput.writeByte(1);

        // fixed width is always written regardless of null flag
        sliceOutput.writeZero(type.getFixedSize());

        entryAdded();

        return this;
    }

    private void entryAdded()
    {
        positionCount++;
        blockBuilderStatus.addBytes(entrySize);
        if (sliceOutput.size() >= blockBuilderStatus.getMaxBlockSizeInBytes()) {
            blockBuilderStatus.setFull();
        }
    }

    @Override
    public Block build()
    {
        return new FixedWidthBlock(type, positionCount, sliceOutput.slice());
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("FixedWidthBlockBuilder{");
        sb.append("positionCount=").append(positionCount);
        sb.append(", size=").append(sliceOutput.size());
        sb.append(", type=").append(type);
        sb.append('}');
        return sb.toString();
    }
}
