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

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.RandomAccessBlock;
import com.facebook.presto.type.Type;
import com.facebook.presto.type.VarcharType;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;

public class VariableWidthBlockCursor
        implements BlockCursor
{
    private final VarcharType type;
    private final int positionCount;
    private final Slice slice;

    private int position;
    private int entryOffset;
    private int entrySize;
    private boolean isNull;

    public VariableWidthBlockCursor(VarcharType type, int positionCount, Slice slice)
    {
        this.type = checkNotNull(type, "type is null");
        this.slice = checkNotNull(slice, "slice is null");
        checkArgument(positionCount >= 0, "positionCount is negative");
        this.positionCount = positionCount;

        entryOffset = 0;
        entrySize = 0;

        // start one position before the start
        position = -1;
    }

    // Accessible for VariableWidthRandomAccessBlock
    int getRawOffset()
    {
        return entryOffset;
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

        nextPosition();
        return true;
    }

    @Override
    public boolean advanceToPosition(int newPosition)
    {
        if (newPosition >= positionCount) {
            position = positionCount;
            return false;
        }

        checkArgument(newPosition >= this.position, "Can't advance backwards");

        // advance to specified position
        while (position < newPosition) {
            nextPosition();
        }
        return true;
    }

    @Override
    public Block getRegionAndAdvance(int length)
    {
        // view port starts at next position
        int startOffset = entryOffset + entrySize;
        length = Math.min(length, getRemainingPositions());

        // advance to end of view port
        for (int i = 0; i < length; i++) {
            nextPosition();
        }

        Slice newSlice = slice.slice(startOffset, entryOffset + entrySize - startOffset);
        return new VariableWidthBlock(type, length, newSlice);
    }

    private void nextPosition()
    {
        position++;
        entryOffset += entrySize;
        isNull = slice.getByte(entryOffset) != 0;
        if (isNull) {
            entrySize = SIZE_OF_BYTE;
        }
        else {
            entrySize = type.getLength(slice, entryOffset + SIZE_OF_BYTE) + SIZE_OF_BYTE;
        }
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

        Slice copy = Slices.allocate(entrySize);
        copy.setBytes(0, slice, entryOffset, entrySize);

        return new VariableWidthRandomAccessBlock(type, 1, copy);
    }

    @Override
    public boolean getBoolean()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getSlice()
    {
        checkReadablePosition();
        return type.getSlice(slice, entryOffset + SIZE_OF_BYTE);
    }

    @Override
    public Object getObjectValue()
    {
        checkReadablePosition();
        if (isNull) {
            return null;
        }
        return type.getObjectValue(slice, entryOffset + SIZE_OF_BYTE);
    }

    @Override
    public boolean isNull()
    {
        checkReadablePosition();
        return isNull;
    }

    @Override
    public int compareTo(Slice rightSlice, int rightOffset)
    {
        checkReadablePosition();
        return type.compareTo(slice, entryOffset + SIZE_OF_BYTE, rightSlice, rightOffset);
    }

    @Override
    public int calculateHashCode()
    {
        checkReadablePosition();
        if (isNull) {
            return 0;
        }
        return type.hashCode(slice, entryOffset + SIZE_OF_BYTE);
    }

    @Override
    public void appendTo(BlockBuilder blockBuilder)
    {
        checkReadablePosition();
        if (isNull) {
            blockBuilder.appendNull();
        }
        else {
            type.appendTo(slice, entryOffset + SIZE_OF_BYTE, blockBuilder);
        }
    }
}
