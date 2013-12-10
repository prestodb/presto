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
package com.facebook.presto.block.rle;

import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.block.RandomAccessBlock;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class RunLengthEncodedBlockCursor
        implements BlockCursor
{
    private final RandomAccessBlock value;
    private final int positionCount;

    private int position = -1;

    public RunLengthEncodedBlockCursor(RandomAccessBlock value, int positionCount)
    {
        this.value = checkNotNull(value, "value is null");
        checkArgument(value.getPositionCount() == 1, "Expected value to contain a single position but has %s positions", value.getPositionCount());

        checkArgument(positionCount >= 0, "positionCount is negative");
        this.positionCount = positionCount;

        position = -1;
    }

    public RunLengthEncodedBlockCursor(RunLengthEncodedBlockCursor cursor)
    {
        this.value = cursor.value;
        this.positionCount = cursor.positionCount;
        this.position = cursor.position;
    }

    @Override
    public BlockCursor duplicate()
    {
        return new RunLengthEncodedBlockCursor(this);
    }

    @Override
    public Type getType()
    {
        return value.getType();
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

        this.position = newPosition;
        return true;
    }

    @Override
    public Block getRegionAndAdvance(int length)
    {
        length = Math.min(length, getRemainingPositions());

        // advance to end of view port
        position += length;

        return new RunLengthEncodedBlock(value, length);
    }

    @Override
    public RandomAccessBlock getSingleValueBlock()
    {
        checkReadablePosition();
        return value;
    }

    @Override
    public boolean getBoolean()
    {
        checkReadablePosition();
        return value.getBoolean(0);
    }

    @Override
    public long getLong()
    {
        checkReadablePosition();
        return value.getLong(0);
    }

    @Override
    public double getDouble()
    {
        checkReadablePosition();
        return value.getDouble(0);
    }

    @Override
    public Slice getSlice()
    {
        checkReadablePosition();
        return value.getSlice(0);
    }

    @Override
    public Object getObjectValue(Session session)
    {
        checkReadablePosition();
        return value.getObjectValue(session, 0);
    }

    @Override
    public boolean isNull()
    {
        checkReadablePosition();
        return value.isNull(0);
    }

    @Override
    public int getPosition()
    {
        checkReadablePosition();
        return position;
    }

    @Override
    public int compareTo(Slice slice, int offset)
    {
        checkReadablePosition();
        return value.compareTo(0, slice, offset);
    }

    @Override
    public int calculateHashCode()
    {
        return value.hashCode(0);
    }

    @Override
    public void appendTo(BlockBuilder blockBuilder)
    {
        value.appendTo(0, blockBuilder);
    }
}
