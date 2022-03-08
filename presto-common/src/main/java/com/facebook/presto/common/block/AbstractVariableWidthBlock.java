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
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;

import static com.facebook.presto.common.block.BlockUtil.checkValidPosition;
import static io.airlift.slice.Slices.EMPTY_SLICE;

public abstract class AbstractVariableWidthBlock
        implements Block
{
    // TODO: create ColumnarSlice class and expose the rawSlice through it
    public abstract Slice getRawSlice(int position);

    // TODO: create ColumnarSlice class and expose the offset through it
    public abstract int getPositionOffset(int position);

    protected abstract boolean isEntryNull(int position);

    @Override
    public String getEncodingName()
    {
        return VariableWidthBlockEncoding.NAME;
    }

    @Override
    public byte getByte(int position)
    {
        checkReadablePosition(position);
        return getRawSlice(position).getByte(getPositionOffset(position));
    }

    @Override
    public short getShort(int position)
    {
        checkReadablePosition(position);
        return getRawSlice(position).getShort(getPositionOffset(position));
    }

    @Override
    public int getInt(int position)
    {
        checkReadablePosition(position);
        return getRawSlice(position).getInt(getPositionOffset(position));
    }

    @Override
    public long getLong(int position)
    {
        checkReadablePosition(position);
        return getRawSlice(position).getLong(getPositionOffset(position));
    }

    @Override
    public long getLong(int position, int offset)
    {
        checkReadablePosition(position);
        return getRawSlice(position).getLong(getPositionOffset(position) + offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        checkReadablePosition(position);
        return getRawSlice(position).slice(getPositionOffset(position) + offset, length);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        checkReadablePosition(position);
        Slice rawSlice = getRawSlice(position);
        if (getSliceLength(position) < length) {
            return false;
        }
        return otherBlock.bytesEqual(otherPosition, otherOffset, rawSlice, getPositionOffset(position) + offset, length);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        checkReadablePosition(position);
        return getRawSlice(position).equals(getPositionOffset(position) + offset, length, otherSlice, otherOffset, length);
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        checkReadablePosition(position);
        return XxHash64.hash(getRawSlice(position), getPositionOffset(position) + offset, length);
    }

    @Override
    public int compareTo(int position, int offset, int length, Block otherBlock, int otherPosition, int otherOffset, int otherLength)
    {
        checkReadablePosition(position);
        Slice rawSlice = getRawSlice(position);
        if (getSliceLength(position) < length) {
            throw new IllegalArgumentException("Length longer than value length");
        }
        return -otherBlock.bytesCompare(otherPosition, otherOffset, otherLength, rawSlice, getPositionOffset(position) + offset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        checkReadablePosition(position);
        return getRawSlice(position).compareTo(getPositionOffset(position) + offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        blockBuilder.writeBytes(getRawSlice(position), getPositionOffset(position) + offset, length);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, SliceOutput sliceOutput)
    {
        checkReadablePosition(position);
        sliceOutput.writeBytes(getRawSlice(position), getPositionOffset(position) + offset, length);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        writeBytesTo(position, 0, getSliceLength(position), blockBuilder);
        blockBuilder.closeEntry();
    }

    @Override
    public void writePositionTo(int position, SliceOutput output)
    {
        if (isNull(position)) {
            output.writeByte(0);
        }
        else {
            output.writeByte(1);
            int sliceLength = getSliceLength(position);
            output.writeInt(sliceLength);
            output.writeBytes(getRawSlice(position), getPositionOffset(position), sliceLength);
        }
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        if (isNull(position)) {
            return new VariableWidthBlock(0, 1, EMPTY_SLICE, new int[] {0, 0}, new boolean[] {true});
        }

        int offset = getPositionOffset(position);
        int entrySize = getSliceLength(position);

        Slice copy = Slices.copyOf(getRawSlice(position), offset, entrySize);

        return new VariableWidthBlock(0, 1, copy, new int[] {0, copy.length()}, null);
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return isNull(position) ? 0 : getSliceLength(position);
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return isEntryNull(position);
    }

    protected void checkReadablePosition(int position)
    {
        checkValidPosition(position, getPositionCount());
    }
}
