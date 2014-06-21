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
import com.facebook.presto.spi.type.FixedWidthType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static java.util.Objects.requireNonNull;

public abstract class AbstractFixedWidthBlock
        implements Block
{
    protected final FixedWidthType type;
    protected final int entrySize;

    protected AbstractFixedWidthBlock(FixedWidthType type)
    {
        this.type = requireNonNull(type, "type is null");
        this.entrySize = type.getFixedSize();
    }

    protected abstract Slice getRawSlice();

    protected abstract boolean isEntryNull(int position);

    @Override
    public FixedWidthType getType()
    {
        return type;
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new FixedWidthBlockEncoding(type);
    }

    @Override
    public boolean getBoolean(int position)
    {
        checkReadablePosition(position);
        return type.getBoolean(getRawSlice(), valueOffset(position));
    }

    @Override
    public long getLong(int position)
    {
        checkReadablePosition(position);
        return type.getLong(getRawSlice(), valueOffset(position));
    }

    @Override
    public double getDouble(int position)
    {
        checkReadablePosition(position);
        return type.getDouble(getRawSlice(), valueOffset(position));
    }

    @Override
    public Object getObjectValue(ConnectorSession session, int position)
    {
        checkReadablePosition(position);
        if (isNull(position)) {
            return null;
        }
        return type.getObjectValue(session, getRawSlice(), valueOffset(position));
    }

    @Override
    public Slice getSlice(int position)
    {
        checkReadablePosition(position);
        return type.getSlice(getRawSlice(), valueOffset(position));
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);

        Slice copy = Slices.copyOf(getRawSlice(), valueOffset(position), entrySize);

        return new FixedWidthBlock(type, 1, copy, new boolean[] {isNull(position)});
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return isEntryNull(position);
    }

    @Override
    public boolean equalTo(int position, Block otherBlock, int otherPosition)
    {
        boolean leftIsNull = isNull(position);
        boolean rightIsNull = otherBlock.isNull(otherPosition);

        if (leftIsNull != rightIsNull) {
            return false;
        }

        // if values are both null, they are equal
        if (leftIsNull) {
            return true;
        }

        return otherBlock.equalTo(otherPosition, getRawSlice(), valueOffset(position));
    }

    @Override
    public boolean equalTo(int position, Slice otherSlice, int otherOffset)
    {
        checkReadablePosition(position);
        return type.equalTo(getRawSlice(), valueOffset(position), otherSlice, otherOffset);
    }

    @Override
    public int hash(int position)
    {
        if (isNull(position)) {
            return 0;
        }
        return type.hash(getRawSlice(), valueOffset(position));
    }

    @Override
    public int compareTo(SortOrder sortOrder, int position, Block otherBlock, int otherPosition)
    {
        boolean leftIsNull = isNull(position);
        boolean rightIsNull = otherBlock.isNull(otherPosition);

        if (leftIsNull && rightIsNull) {
            return 0;
        }
        if (leftIsNull) {
            return sortOrder.isNullsFirst() ? -1 : 1;
        }
        if (rightIsNull) {
            return sortOrder.isNullsFirst() ? 1 : -1;
        }

        // compare the right block to our slice but negate the result since we are evaluating in the opposite order
        int result = -otherBlock.compareTo(otherPosition, getRawSlice(), valueOffset(position));
        return sortOrder.isAscending() ? result : -result;
    }

    @Override
    public int compareTo(int position, Slice otherSlice, int otherOffset)
    {
        checkReadablePosition(position);
        return type.compareTo(getRawSlice(), valueOffset(position), otherSlice, otherOffset);
    }

    @Override
    public void appendTo(int position, BlockBuilder blockBuilder)
    {
        if (isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            type.appendTo(getRawSlice(), valueOffset(position), blockBuilder);
        }
    }

    private int valueOffset(int position)
    {
        return position * entrySize;
    }

    protected void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }
}
