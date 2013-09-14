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

public class UncompressedBlockCursor
        implements BlockCursor
{
    private final TupleInfo tupleInfo;
    private final Slice slice;
    private final int positionCount;

    private int position;
    private int offset;
    private int size;

    public UncompressedBlockCursor(TupleInfo tupleInfo, int positionCount, Slice slice)
    {
        Preconditions.checkNotNull(tupleInfo, "tupleInfo is null");
        Preconditions.checkArgument(positionCount >= 0, "positionCount is negative");
        Preconditions.checkNotNull(positionCount, "positionCount is null");

        this.tupleInfo = tupleInfo;
        this.positionCount = positionCount;

        this.slice = slice;
        offset = 0;
        size = 0;

        // start one position before the start
        position = -1;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
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
        offset += size;
        size = tupleInfo.size(slice, offset);
        return true;
    }

    @Override
    public boolean advanceToPosition(int newPosition)
    {
        if (newPosition >= positionCount) {
            position = positionCount;
            return false;
        }

        Preconditions.checkArgument(newPosition >= this.position, "Can't advance backwards");

        // advance to specified position
        while (position < newPosition) {
            position++;
            offset += size;
            size = tupleInfo.size(slice, offset);
        }
        return true;
    }

    @Override
    public Block getRegionAndAdvance(int length)
    {
        // view port starts at next position
        int startOffset = offset + size;
        length = Math.min(length, getRemainingPositions());

        // advance to end of view port
        for (int i = 0; i < length; i++) {
            position++;
            offset += size;
            size = tupleInfo.size(slice, offset);
        }
        Slice newSlice = slice.slice(startOffset, offset + size - startOffset);
        return new UncompressedBlock(length, tupleInfo, newSlice);
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
        Slice copy = Slices.allocate(size);
        copy.setBytes(0, slice, offset, size);

        return new Tuple(copy, tupleInfo);
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkReadablePosition();
        return tupleInfo.getBoolean(slice, offset, field);
    }

    @Override
    public long getLong(int field)
    {
        checkReadablePosition();
        return tupleInfo.getLong(slice, offset, field);
    }

    @Override
    public double getDouble(int field)
    {
        checkReadablePosition();
        return tupleInfo.getDouble(slice, offset, field);
    }

    @Override
    public Slice getSlice(int field)
    {
        checkReadablePosition();
        return tupleInfo.getSlice(slice, offset, field);
    }

    @Override
    public boolean isNull(int field)
    {
        checkReadablePosition();
        return tupleInfo.isNull(slice, offset, field);
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        checkReadablePosition();
        Slice tupleSlice = value.getTupleSlice();
        return slice.equals(offset, size, tupleSlice, 0, tupleSlice.length());
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
        blockBuilder.appendTuple(slice, offset, size);
    }
}
