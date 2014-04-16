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

import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.type.FixedWidthType;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static java.util.Objects.requireNonNull;

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
        this.type = requireNonNull(type, "type is null");
        this.entrySize = type.getFixedSize() + SIZE_OF_BYTE;

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        this.slice = requireNonNull(slice, "slice is null");

        // start one position before the start
        position = -1;
        offset = -entrySize;
    }

    public FixedWidthBlockCursor(FixedWidthBlockCursor cursor)
    {
        this.type = cursor.type;
        this.entrySize = cursor.entrySize;
        this.slice = cursor.slice;
        this.positionCount = cursor.positionCount;
        this.position = cursor.position;
        this.offset = cursor.offset;
    }

    @Override
    public FixedWidthBlockCursor duplicate()
    {
        return new FixedWidthBlockCursor(this);
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

        if (!(newPosition >= position)) {
            throw new IllegalArgumentException("Can't advance backwards");
        }

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
        return type.getBoolean(slice, valueOffset());
    }

    @Override
    public long getLong()
    {
        checkReadablePosition();
        return type.getLong(slice, valueOffset());
    }

    @Override
    public double getDouble()
    {
        checkReadablePosition();
        return type.getDouble(slice, valueOffset());
    }

    @Override
    public Slice getSlice()
    {
        checkReadablePosition();
        return type.getSlice(slice, valueOffset());
    }

    @Override
    public Object getObjectValue(Session session)
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
        return slice.getByte(offset) != 0;
    }

    @Override
    public int compareTo(Slice rightSlice, int rightOffset)
    {
        checkReadablePosition();
        return type.compareTo(slice, valueOffset(), rightSlice, rightOffset);
    }

    @Override
    public int calculateHashCode()
    {
        if (isNull()) {
            return 0;
        }
        return type.hashCode(slice, valueOffset());
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

    private int valueOffset()
    {
        return offset + SIZE_OF_BYTE;
    }
}
