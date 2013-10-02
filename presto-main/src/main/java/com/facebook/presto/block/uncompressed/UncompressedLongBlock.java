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
import com.facebook.presto.operator.SortOrder;
import com.facebook.presto.serde.BlockEncoding;
import com.facebook.presto.serde.UncompressedBlockEncoding;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import com.google.common.base.Objects;
import com.google.common.primitives.Longs;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

public class UncompressedLongBlock
        implements RandomAccessBlock
{
    private static final int ENTRY_SIZE = SIZE_OF_LONG + SIZE_OF_BYTE;
    private final Slice slice;
    private final int positionCount;

    public UncompressedLongBlock(int positionCount, Slice slice)
    {
        checkArgument(positionCount >= 0, "positionCount is negative");
        checkNotNull(positionCount, "positionCount is null");

        this.positionCount = positionCount;

        this.slice = slice;
    }

    public UncompressedLongBlock(UncompressedLongBlock block)
    {
        checkNotNull(block, "block is null");
        slice = block.slice;
        positionCount = block.positionCount;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return TupleInfo.SINGLE_LONG;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public DataSize getDataSize()
    {
        return new DataSize(slice.length(), Unit.BYTE);
    }

    @Override
    public BlockCursor cursor()
    {
        return new UncompressedLongBlockCursor(positionCount, slice);
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new UncompressedBlockEncoding(TupleInfo.SINGLE_LONG);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkPositionIndexes(positionOffset, positionOffset + length, positionCount);
        return cursor().getRegionAndAdvance(length);
    }

    @Override
    public RandomAccessBlock toRandomAccessBlock()
    {
        return this;
    }

    @Override
    public boolean getBoolean(int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int position)
    {
        checkReadablePosition(position);
        return slice.getLong((position * ENTRY_SIZE) + SIZE_OF_BYTE);
    }

    @Override
    public double getDouble(int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getSlice(int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean sliceEquals(int rightPosition, Slice slice, int offset, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int sliceCompareTo(int leftPosition, Slice rightSlice, int rightOffset, int rightLength)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return slice.getByte((position * ENTRY_SIZE)) != 0;
    }

    @Override
    public boolean equals(int position, RandomAccessBlock right, int rightPosition)
    {
        checkReadablePosition(position);
        int entryOffset = position * ENTRY_SIZE;
        boolean leftIsNull = slice.getByte(entryOffset) != 0;
        boolean rightIsNull = right.isNull(rightPosition);

        if (leftIsNull != rightIsNull) {
            return false;
        }

        // if values are both null, they are equal
        if (leftIsNull) {
            return true;
        }
        return slice.getLong(entryOffset + SIZE_OF_BYTE) == right.getLong(rightPosition);
    }

    @Override
    public boolean equals(int position, TupleReadable value)
    {
        checkReadablePosition(position);
        int entryOffset = position * ENTRY_SIZE;
        boolean thisIsNull = slice.getByte(entryOffset) != 0;
        boolean valueIsNull = value.isNull();

        if (thisIsNull != valueIsNull) {
            return false;
        }

        // if values are both null, they are equal
        if (thisIsNull) {
            return true;
        }
        return slice.getLong(entryOffset + SIZE_OF_BYTE) == value.getLong();
    }

    @Override
    public int hashCode(int position)
    {
        checkReadablePosition(position);
        int entryOffset = position * ENTRY_SIZE;
        if (slice.getByte(entryOffset) != 0) {
            return 0;
        }
        else {
            return Longs.hashCode(slice.getLong(entryOffset + SIZE_OF_BYTE));
        }
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, RandomAccessBlock right, int rightPosition)
    {
        checkReadablePosition(position);
        int entryOffset = position * ENTRY_SIZE;
        boolean leftIsNull = slice.getByte(entryOffset) != 0;
        boolean rightIsNull = right.isNull(rightPosition);

        if (leftIsNull && rightIsNull) {
            return 0;
        }
        if (leftIsNull) {
            return sortOrder.isNullsFirst() ? -1 : 1;
        }
        if (rightIsNull) {
            return sortOrder.isNullsFirst() ? 1 : -1;
        }

        int result = Long.compare(slice.getLong(entryOffset + SIZE_OF_BYTE), right.getLong(rightPosition));
        return sortOrder.isAscending() ? result : -result;
    }

    @Override
    public void appendTupleTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        int entryOffset = position * ENTRY_SIZE;
        if (slice.getByte(entryOffset) != 0) {
            blockBuilder.appendNull();
        }
        else {
            blockBuilder.append(slice.getLong(entryOffset + SIZE_OF_BYTE));
        }
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("positionCount", positionCount)
                .add("slice", slice)
                .toString();
    }

    private void checkReadablePosition(int position)
    {
        checkState(position >= 0 && position < positionCount, "position is not valid");
    }
}
