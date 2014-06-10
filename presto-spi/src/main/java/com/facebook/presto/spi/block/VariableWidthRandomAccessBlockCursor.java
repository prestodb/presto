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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VariableWidthType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static java.util.Objects.requireNonNull;

public class VariableWidthRandomAccessBlockCursor
        implements BlockCursor
{
    private final VariableWidthType type;
    private final Slice slice;
    private final int[] offsets;
    private final int positionCount;

    private int position = -1;

    public VariableWidthRandomAccessBlockCursor(VariableWidthType type, int positionCount, Slice slice, int[] offsets)
    {
        this.type = requireNonNull(type, "type is null");

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        this.slice = requireNonNull(slice, "slice is null");
        this.offsets = requireNonNull(offsets, "offsets is null");

        // start one position before the start
        position = -1;
    }

    public VariableWidthRandomAccessBlockCursor(VariableWidthRandomAccessBlockCursor cursor)
    {
        this.type = cursor.type;
        this.slice = cursor.slice;
        this.offsets = cursor.offsets;
        this.positionCount = cursor.positionCount;
        this.position = cursor.position;
    }

    @Override
    public VariableWidthRandomAccessBlockCursor duplicate()
    {
        return new VariableWidthRandomAccessBlockCursor(this);
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
        if (!isValid()) {
            throw new IllegalStateException("cursor is not valid");
        }
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (position >= positionCount - 1) {
            position = positionCount;
            return false;
        }

        position++;
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

        if (newPosition < position) {
            throw new IllegalArgumentException("Can't advance backwards");
        }

        position = newPosition;

        return true;
    }

    @Override
    public Block getRegionAndAdvance(int length)
    {
        // view port starts at next position
        int startPosition = position + 1;
        length = Math.min(length, getRemainingPositions());

        // advance to end of view port
        position += length;

        int[] newOffsets = Arrays.copyOfRange(offsets, startPosition, startPosition + length);
        return new VariableWidthRandomAccessBlock(type, length, slice, newOffsets);
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

        if (isNull()) {
            return new VariableWidthRandomAccessBlock(type, 1, Slices.wrappedBuffer(new byte[] {1}), new int[] {0});
        }

        Slice copy = Slices.copyOf(slice, offsets[position], entrySize());

        return new VariableWidthRandomAccessBlock(type, 1, copy, new int[] {0});
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
        return type.getSlice(slice, valueOffset());
    }

    @Override
    public Object getObjectValue(ConnectorSession session)
    {
        checkReadablePosition();
        if (isNull()) {
            return null;
        }
        return type.getObjectValue(session, slice, valueOffset());
    }

    @Override
    public boolean isNull()
    {
        checkReadablePosition();
        int offset = offsets[position];
        return isEntryAtOffsetNull(offset);
    }

    @Override
    public int compareTo(Slice otherSlice, int otherOffset)
    {
        checkReadablePosition();
        return type.compareTo(slice, valueOffset(), otherSlice, otherOffset);
    }

    @Override
    public int hash()
    {
        if (isNull()) {
            return 0;
        }
        return type.hash(slice, valueOffset());
    }

    @Override
    public void appendTo(BlockBuilder blockBuilder)
    {
        if (isNull()) {
            blockBuilder.appendNull();
        }
        else {
            type.appendTo(slice, valueOffset(), blockBuilder);
        }
    }

    private boolean isEntryAtOffsetNull(int offset)
    {
        return slice.getByte(offset) != 0;
    }

    private int valueOffset()
    {
        return offsets[position] + SIZE_OF_BYTE;
    }

    public int entrySize()
    {
        return type.getLength(slice, offsets[position] + SIZE_OF_BYTE) + SIZE_OF_BYTE;
    }
}
