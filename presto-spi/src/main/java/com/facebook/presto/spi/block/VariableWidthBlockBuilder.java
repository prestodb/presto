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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VariableWidthType;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import java.util.Arrays;
import java.util.Objects;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;

public class VariableWidthBlockBuilder
        extends AbstractVariableWidthRandomAccessBlock
        implements BlockBuilder
{
    private final BlockBuilderStatus blockBuilderStatus;
    private final SliceOutput sliceOutput;

    private int positions;
    private int[] offsets = new int[1024];

    public VariableWidthBlockBuilder(VariableWidthType type, BlockBuilderStatus blockBuilderStatus)
    {
        super(type);

        this.blockBuilderStatus = Objects.requireNonNull(blockBuilderStatus, "blockBuilderStatus is null");
        this.sliceOutput = new DynamicSliceOutput((int) (blockBuilderStatus.getMaxBlockSizeInBytes() * 1.2));
    }

    @Override
    protected int getPositionOffset(int position)
    {
        if (position >= positions) {
            throw new IllegalArgumentException("position " + position + " must be less than position count " + positions);
        }
        return offsets[position];
    }

    @Override
    protected int[] getOffsets()
    {
        return offsets;
    }

    @Override
    protected Slice getRawSlice()
    {
        return sliceOutput.getUnderlyingSlice();
    }

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public int getPositionCount()
    {
        return positions;
    }

    @Override
    public boolean isEmpty()
    {
        return positions == 0;
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
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder appendLong(long value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder appendDouble(double value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder appendSlice(Slice value)
    {
        return appendSlice(value, 0, value.length());
    }

    @Override
    public BlockBuilder appendSlice(Slice value, int offset, int length)
    {
        recordNewPosition();

        sliceOutput.writeByte(0);

        int bytesWritten = type.writeSlice(sliceOutput, value, offset, length);

        entryAdded(bytesWritten);

        return this;
    }

    @Override
    public BlockBuilder appendNull()
    {
        recordNewPosition();

        sliceOutput.writeByte(1);

        entryAdded(0);

        return this;
    }

    private void entryAdded(int bytesWritten)
    {
        blockBuilderStatus.addBytes(SIZE_OF_BYTE + bytesWritten);
        if (sliceOutput.size() >= blockBuilderStatus.getMaxBlockSizeInBytes()) {
            blockBuilderStatus.setFull();
        }
    }

    private void recordNewPosition()
    {
        if (positions == offsets.length) {
            offsets = Arrays.copyOf(offsets, offsets.length * 2);
        }

        offsets[positions] = sliceOutput.size();
        positions++;
    }

    @Override
    public Block build()
    {
        return new VariableWidthRandomAccessBlock(type, positions, sliceOutput.slice(), Arrays.copyOf(offsets, positions));
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("VariableWidthBlockBuilder{");
        sb.append("positionCount=").append(positions);
        sb.append(", size=").append(sliceOutput.size());
        sb.append(", type=").append(type);
        sb.append('}');
        return sb.toString();
    }
}
