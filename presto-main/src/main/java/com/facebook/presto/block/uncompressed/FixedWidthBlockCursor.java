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
import com.facebook.presto.tuple.FixedWidthTypeInfo;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;

public class FixedWidthBlockCursor
        implements BlockCursor
{
    private final FixedWidthTypeInfo typeInfo;
    private final int entrySize;
    private final Slice slice;
    private final int positionCount;

    private int position;
    private int offset;

    public FixedWidthBlockCursor(FixedWidthTypeInfo typeInfo, int positionCount, Slice slice)
    {
        this.typeInfo = checkNotNull(typeInfo, "typeInfo is null");
        this.entrySize = typeInfo.getSize() + SIZE_OF_BYTE;

        checkArgument(positionCount >= 0, "positionCount is negative");
        this.positionCount = positionCount;

        this.slice = checkNotNull(slice, "slice is null");

        // start one position before the start
        position = -1;
        offset = -entrySize;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return new TupleInfo(typeInfo.getType());
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
        return new FixedWidthBlock(typeInfo, length, newSlice);
    }

    @Override
    public int getPosition()
    {
        checkReadablePosition();
        return position;
    }

    @Override
    public Tuple getTuple()
    {
        checkReadablePosition();

        // TODO: add Slices.copyOf() to airlift
        Slice copy = Slices.allocate(entrySize);
        copy.setBytes(0, slice, offset, entrySize);

        return new Tuple(copy, new TupleInfo(typeInfo.getType()));
    }

    @Override
    public boolean getBoolean()
    {
        checkReadablePosition();
        return typeInfo.getBoolean(slice, offset + SIZE_OF_BYTE);
    }

    @Override
    public long getLong()
    {
        checkReadablePosition();
        return typeInfo.getLong(slice, offset + SIZE_OF_BYTE);
    }

    @Override
    public double getDouble()
    {
        checkReadablePosition();
        return typeInfo.getDouble(slice, offset + SIZE_OF_BYTE);
    }

    @Override
    public Slice getSlice()
    {
        checkReadablePosition();
        return typeInfo.getSlice(slice, offset + SIZE_OF_BYTE);
    }

    @Override
    public boolean isNull()
    {
        checkReadablePosition();
        return slice.getByte(offset) != 0;
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        checkReadablePosition();
        boolean thisIsNull = slice.getByte(offset) != 0;
        boolean valueIsNull = value.isNull();

        if (thisIsNull != valueIsNull) {
            return false;
        }

        // if values are both null, they are equal
        if (thisIsNull) {
            return true;
        }
        return typeInfo.equals(slice, offset + SIZE_OF_BYTE, value);
    }

    @Override
    public void appendTupleTo(BlockBuilder blockBuilder)
    {
        checkReadablePosition();
        if (slice.getByte(offset) != 0) {
            blockBuilder.appendNull();
        }
        else {
            typeInfo.appendTo(slice, offset + SIZE_OF_BYTE, blockBuilder);
        }
    }

    @Override
    public int getRawOffset()
    {
        return offset;
    }

    @Override
    public Slice getRawSlice()
    {
        return slice;
    }
}
