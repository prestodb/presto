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
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.Tuples;
import com.facebook.presto.tuple.VariableWidthTypeInfo;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;

public class VariableWidthBlockCursor
        implements BlockCursor
{
    private final VariableWidthTypeInfo typeInfo;
    private final int positionCount;
    private final Slice slice;

    private int position;
    private int entryOffset;
    private int entrySize;
    private boolean isNull;

    public VariableWidthBlockCursor(VariableWidthTypeInfo typeInfo, int positionCount, Slice slice)
    {
        this.typeInfo = checkNotNull(typeInfo, "typeInfo is null");
        this.slice = checkNotNull(slice, "slice is null");
        checkArgument(positionCount >= 0, "positionCount is negative");
        this.positionCount = positionCount;

        entryOffset = 0;
        entrySize = 0;

        // start one position before the start
        position = -1;
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
        return new VariableWidthBlock(typeInfo, length, newSlice);
    }

    private void nextPosition()
    {
        position++;
        entryOffset += entrySize;
        isNull = slice.getByte(entryOffset) != 0;
        if (isNull) {
            entrySize = 1;
        }
        else {
            entrySize = typeInfo.getLength(slice, entryOffset + SIZE_OF_BYTE) + SIZE_OF_BYTE;
        }
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

        if (isNull) {
            return Tuples.NULL_STRING_TUPLE;
        }
        else {
            Slice copy = Slices.allocate(entrySize);
            copy.setBytes(0, slice, entryOffset, entrySize);

            return new Tuple(copy, SINGLE_VARBINARY);
        }
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
        return typeInfo.getSlice(slice, entryOffset + SIZE_OF_BYTE);
    }

    @Override
    public boolean isNull()
    {
        checkReadablePosition();
        return isNull;
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        checkReadablePosition();

        boolean thisIsNull = isNull;
        boolean valueIsNull = value.isNull();

        if (thisIsNull != valueIsNull) {
            return false;
        }

        // if values are both null, they are equal
        if (thisIsNull) {
            return true;
        }
        return typeInfo.equals(slice, entryOffset + SIZE_OF_BYTE, value);
    }

    @Override
    public int getRawOffset()
    {
        return entryOffset;
    }

    @Override
    public Slice getRawSlice()
    {
        return slice;
    }

    @Override
    public void appendTupleTo(BlockBuilder blockBuilder)
    {
        checkReadablePosition();
        if (isNull) {
            blockBuilder.appendNull();
        }
        else {
            typeInfo.appendTo(slice, entryOffset + SIZE_OF_BYTE, blockBuilder);
        }
    }
}
