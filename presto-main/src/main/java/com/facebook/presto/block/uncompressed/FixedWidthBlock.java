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

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.RandomAccessBlock;
import com.facebook.presto.operator.SortOrder;
import com.facebook.presto.serde.BlockEncoding;
import com.facebook.presto.serde.UncompressedBlockEncoding;
import com.facebook.presto.tuple.FixedWidthTypeInfo;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import com.google.common.base.Objects;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;

public class FixedWidthBlock
        implements RandomAccessBlock
{
    private final FixedWidthTypeInfo typeInfo;
    private final int entrySize;
    private final Slice slice;
    private final int positionCount;

    public FixedWidthBlock(FixedWidthTypeInfo typeInfo, int positionCount, Slice slice)
    {
        this.typeInfo = checkNotNull(typeInfo, "typeInfo is null");
        this.entrySize = typeInfo.getSize() + SIZE_OF_BYTE;

        checkArgument(positionCount >= 0, "positionCount is negative");
        this.positionCount = positionCount;

        this.slice = checkNotNull(slice, "slice is null");
    }

    public FixedWidthBlock(FixedWidthBlock block)
    {
        checkNotNull(block, "block is null");
        typeInfo = block.typeInfo;
        entrySize = block.entrySize;
        slice = block.slice;
        positionCount = block.positionCount;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return new TupleInfo(typeInfo.getType());
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
        return new FixedWidthBlockCursor(typeInfo, positionCount, slice);
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new UncompressedBlockEncoding(new TupleInfo(typeInfo.getType()));
    }

    @Override
    public RandomAccessBlock getRegion(int positionOffset, int length)
    {
        checkPositionIndexes(positionOffset, positionOffset + length, positionCount);
        return (RandomAccessBlock) cursor().getRegionAndAdvance(length);
    }

    @Override
    public RandomAccessBlock toRandomAccessBlock()
    {
        return this;
    }

    @Override
    public boolean getBoolean(int position)
    {
        checkReadablePosition(position);
        return typeInfo.getBoolean(slice, (position * entrySize) + SIZE_OF_BYTE);
    }

    @Override
    public long getLong(int position)
    {
        checkReadablePosition(position);
        return typeInfo.getLong(slice, (position * entrySize) + SIZE_OF_BYTE);
    }

    @Override
    public double getDouble(int position)
    {
        checkReadablePosition(position);
        return typeInfo.getDouble(slice, (position * entrySize) + SIZE_OF_BYTE);
    }

    @Override
    public Slice getSlice(int position)
    {
        checkReadablePosition(position);
        return typeInfo.getSlice(slice, (position * entrySize) + SIZE_OF_BYTE);
    }

    @Override
    public Tuple getTuple(int position)
    {
        checkReadablePosition(position);
        int entryOffset = position * entrySize;

        // TODO: add Slices.copyOf() to airlift
        Slice copy = Slices.allocate(entrySize);
        copy.setBytes(0, slice, entryOffset, entrySize);

        return new Tuple(copy, new TupleInfo(typeInfo.getType()));
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return slice.getByte((position * entrySize)) != 0;
    }

    @Override
    public boolean equals(int position, RandomAccessBlock rightBlock, int rightPosition)
    {
        FixedWidthBlock right = (FixedWidthBlock) rightBlock;

        checkReadablePosition(position);
        int leftEntryOffset = position * entrySize;
        boolean leftIsNull = slice.getByte(leftEntryOffset) != 0;

        right.checkReadablePosition(rightPosition);
        int rightEntryOffset = rightPosition * entrySize;
        boolean rightIsNull = right.slice.getByte(rightEntryOffset) != 0;

        if (leftIsNull != rightIsNull) {
            return false;
        }

        // if values are both null, they are equal
        if (leftIsNull) {
            return true;
        }

        return typeInfo.equals(slice, leftEntryOffset + SIZE_OF_BYTE, right.slice, rightEntryOffset + SIZE_OF_BYTE);
    }

    @Override
    public boolean equals(int position, TupleReadable value)
    {
        checkReadablePosition(position);
        int entryOffset = position * entrySize;
        boolean thisIsNull = slice.getByte(entryOffset) != 0;
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
    public int hashCode(int position)
    {
        checkReadablePosition(position);
        int entryOffset = position * entrySize;
        if (slice.getByte(entryOffset) != 0) {
            return 0;
        }
        else {
            return typeInfo.hashCode(slice, entryOffset + SIZE_OF_BYTE);
        }
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, RandomAccessBlock rightBlock, int rightPosition)
    {
        FixedWidthBlock right = (FixedWidthBlock) rightBlock;

        checkReadablePosition(position);
        int leftEntryOffset = position * entrySize;
        boolean leftIsNull = slice.getByte(leftEntryOffset) != 0;

        right.checkReadablePosition(rightPosition);
        int rightEntryOffset = rightPosition * entrySize;
        boolean rightIsNull = right.slice.getByte(rightEntryOffset) != 0;

        if (leftIsNull && rightIsNull) {
            return 0;
        }
        if (leftIsNull) {
            return sortOrder.isNullsFirst() ? -1 : 1;
        }
        if (rightIsNull) {
            return sortOrder.isNullsFirst() ? 1 : -1;
        }

        int result = typeInfo.compareTo(slice, leftEntryOffset + SIZE_OF_BYTE, right.slice, rightEntryOffset + SIZE_OF_BYTE);
        return sortOrder.isAscending() ? result : -result;
    }

    @Override
    public void appendTupleTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        int entryOffset = position * entrySize;
        if (slice.getByte(entryOffset) != 0) {
            blockBuilder.appendNull();
        }
        else {
            typeInfo.appendTo(slice, entryOffset + SIZE_OF_BYTE, blockBuilder);
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

    @Override
    public Slice getRawSlice()
    {
        return slice;
    }
}
