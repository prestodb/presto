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
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;

public class UncompressedDoubleBlockCursor
        implements BlockCursor
{
    private static final int ENTRY_SIZE = SIZE_OF_DOUBLE + SIZE_OF_BYTE;
    private final Slice slice;
    private final int positionCount;

    private int position;
    private int offset;

    public UncompressedDoubleBlockCursor(int positionCount, Slice slice)
    {
        Preconditions.checkArgument(positionCount >= 0, "positionCount is negative");
        Preconditions.checkNotNull(positionCount, "positionCount is null");

        this.positionCount = positionCount;

        this.slice = slice;

        // start one position before the start
        position = -1;
        offset = -ENTRY_SIZE;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return TupleInfo.SINGLE_DOUBLE;
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
        offset += ENTRY_SIZE;
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

        offset += (newPosition - position) * ENTRY_SIZE;
        position = newPosition;

        return true;
    }

    @Override
    public Block getRegionAndAdvance(int length)
    {
        // view port starts at next position
        int startOffset = offset + ENTRY_SIZE;
        length = Math.min(length, getRemainingPositions());

        // advance to end of view port
        offset += length * ENTRY_SIZE;
        position += length;

        Slice newSlice = slice.slice(startOffset, length * ENTRY_SIZE);
        return new UncompressedBlock(length, SINGLE_DOUBLE, newSlice);
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
        Slice copy = Slices.allocate(ENTRY_SIZE);
        copy.setBytes(0, slice, offset, ENTRY_SIZE);

        return new Tuple(copy, TupleInfo.SINGLE_DOUBLE);
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
        checkReadablePosition();
        return slice.getDouble(offset + SIZE_OF_BYTE);
    }

    @Override
    public Slice getSlice()
    {
        throw new UnsupportedOperationException();
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
        Slice tupleSlice = value.getTupleSlice();
        return tupleSlice.length() == ENTRY_SIZE && slice.getDouble(offset + SIZE_OF_BYTE) == tupleSlice.getDouble(SIZE_OF_BYTE);
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

    @Override
    public void appendTupleTo(BlockBuilder blockBuilder)
    {
        blockBuilder.appendTuple(slice, offset, ENTRY_SIZE);
    }
}
