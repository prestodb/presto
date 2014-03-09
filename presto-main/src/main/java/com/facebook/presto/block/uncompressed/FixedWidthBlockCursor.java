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
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.block.RandomAccessBlock;
import com.facebook.presto.type.FixedWidthType;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;

public class FixedWidthBlockCursor
        implements BlockCursor
{
    private final FixedWidthType type;
    private final int entrySize;
    private final Slice slice;
    private final int positionCount;

    private int position;
    private int offset;

    public FixedWidthBlockCursor(FixedWidthType type, int positionCount, Slice slice)
    {
        this.type = checkNotNull(type, "type is null");
        this.entrySize = type.getFixedSize() + SIZE_OF_BYTE;

        checkArgument(positionCount >= 0, "positionCount is negative");
        this.positionCount = positionCount;

        this.slice = checkNotNull(slice, "slice is null");

        // start one position before the start
        position = -1;
        offset = -entrySize;
    }

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public int getRemainingPositions()
    {
        return positionCount - (position + 1);
    }

    @Override
    public boolean isValid()
    {
        return 0 <= position && position < positionCount;
    }

    @Override
    public boolean isFinished()
    {
        return position >= positionCount;
    }

    private void checkReadablePosition()
    {
        Preconditions.checkState(isValid(), "cursor is not valid");
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (position >= positionCount - 1) {
            position = positionCount;
            return false;
        }

        position++;
        offset += entrySize;
        return true;
    }

    @Override
    public boolean advanceToPosition(int newPosition)
    {
        // if new position is out of range, return false
        if (newPosition >= positionCount) {
            position = positionCount;
            return false;
        }

        Preconditions.checkArgument(newPosition >= this.position, "Can't advance backwards");

        offset += (newPosition - position) * entrySize;
        position = newPosition;

        return true;
    }

    @Override
    public Block getRegionAndAdvance(int length)
    {
        // view port starts at next position
        int startOffset = offset + entrySize;
        length = Math.min(length, getRemainingPositions());

        // advance to end of view port
        offset += length * entrySize;
        position += length;

        Slice newSlice = slice.slice(startOffset, length * entrySize);
        return new FixedWidthBlock(type, length, newSlice);
    }

    @Override
    public int getPosition()
    {
        checkReadablePosition();
        return position;
    }

    @Override
    public RandomAccessBlock getSingleValueBlock()
    {
        checkReadablePosition();

        // TODO: add Slices.copyOf() to airlift
        Slice copy = Slices.allocate(entrySize);
        copy.setBytes(0, slice, offset, entrySize);

        return new FixedWidthBlock(type, 1, copy);
    }

    @Override
    public boolean getBoolean()
    {
        checkReadablePosition();
        return type.getBoolean(slice, offset + SIZE_OF_BYTE);
    }

    @Override
    public long getLong()
    {
        checkReadablePosition();
        return type.getLong(slice, offset + SIZE_OF_BYTE);
    }

    @Override
    public double getDouble()
    {
        checkReadablePosition();
        return type.getDouble(slice, offset + SIZE_OF_BYTE);
    }

    @Override
    public Slice getSlice()
    {
        checkReadablePosition();
        return type.getSlice(slice, offset + SIZE_OF_BYTE);
    }

    @Override
    public Object getObjectValue()
    {
        checkReadablePosition();
        if (isNull()) {
            return null;
        }
        return type.getObjectValue(slice, offset + SIZE_OF_BYTE);
    }

    @Override
    public boolean isNull()
    {
        checkReadablePosition();
        return slice.getByte(offset) != 0;
    }

    @Override
    public int compareTo(Slice rightSlice, int rightOffset)
    {
        checkReadablePosition();
        return type.compareTo(slice, offset + SIZE_OF_BYTE, rightSlice, rightOffset);
    }

    @Override
    public int calculateHashCode()
    {
        checkReadablePosition();
        if (slice.getByte(offset) != 0) {
            return 0;
        }
        return type.hashCode(slice, offset + SIZE_OF_BYTE);
    }

    @Override
    public void appendTo(BlockBuilder blockBuilder)
    {
        checkReadablePosition();
        if (slice.getByte(offset) != 0) {
            blockBuilder.appendNull();
        }
        else {
            type.appendTo(slice, offset + SIZE_OF_BYTE, blockBuilder);
        }
    }
}
