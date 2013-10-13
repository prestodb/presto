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
package com.facebook.presto.type;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.spi.ColumnType;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Longs;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class FixedWidthType
        implements Type
{
    private final String name;
    private final int fixedSize;
    private final ColumnType type;

    public FixedWidthType(String name, int fixedSize, ColumnType type)
    {
        this.name = name;
        this.fixedSize = fixedSize;
        this.type = type;
    }

    @Override
    public String getName()
    {
        return name;
    }

    public int getFixedSize()
    {
        return fixedSize;
    }

    @Override
    public ColumnType toColumnType()
    {
        return type;
    }

    @Override
    public Object getObjectValue(Slice slice, int offset)
    {
        if (type == ColumnType.LONG) {
            return slice.getLong(offset);
        }
        else if (type == ColumnType.DOUBLE) {
            return slice.getDouble(offset);
        }
        else if (type == ColumnType.BOOLEAN) {
            return slice.getByte(offset) != 0;
        }
        else {
            throw new IllegalArgumentException("Unsupported type " + type);
        }
    }

    public boolean getBoolean(Slice slice, int offset)
    {
        checkState(type == ColumnType.BOOLEAN, "Expected BOOLEAN, but is %s", type);

        return slice.getByte(offset) != 0;
    }

    public void setBoolean(SliceOutput sliceOutput, boolean value)
    {
        checkState(type == ColumnType.BOOLEAN, "Expected BOOLEAN, but is %s", type);

        sliceOutput.writeByte(value ? 1 : 0);
    }

    public long getLong(Slice slice, int offset)
    {
        checkState(type == ColumnType.LONG, "Expected FIXED_INT_64, but is %s", type);

        return slice.getLong(offset);
    }

    public void setLong(SliceOutput sliceOutput, long value)
    {
        checkState(type == ColumnType.LONG, "Expected FIXED_INT_64, but is %s", type);

        sliceOutput.writeLong(value);
    }

    public double getDouble(Slice slice, int offset)
    {
        checkState(type == ColumnType.DOUBLE, "Expected DOUBLE, but is %s", type);

        return slice.getDouble(offset);
    }

    public void setDouble(SliceOutput sliceOutput, double value)
    {
        checkState(type == ColumnType.DOUBLE, "Expected DOUBLE, but is %s", type);

        sliceOutput.writeDouble(value);
    }

    public Slice getSlice(Slice slice, int offset)
    {
        return slice.slice(offset, getFixedSize());
    }

    public void setSlice(SliceOutput sliceOutput, Slice value, int offset, int length)
    {
        checkArgument(length == fixedSize);
        sliceOutput.writeBytes(value, offset, length);
    }

    public boolean equals(Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset)
    {
        if (type == ColumnType.LONG || type == ColumnType.DOUBLE) {
            long leftValue = leftSlice.getLong(leftOffset);
            long rightValue = rightSlice.getLong(rightOffset);
            return leftValue == rightValue;
        }
        else if (type == ColumnType.BOOLEAN) {
            boolean leftValue = leftSlice.getByte(leftOffset) != 0;
            boolean rightValue = rightSlice.getByte(rightOffset) != 0;
            return leftValue == rightValue;
        }
        else {
            throw new IllegalArgumentException("Unsupported type " + type);
        }
    }

    public boolean equals(Slice leftSlice, int leftOffset, BlockCursor rightCursor)
    {
        if (type == ColumnType.LONG) {
            long leftValue = leftSlice.getLong(leftOffset);
            long rightValue = rightCursor.getLong();
            return leftValue == rightValue;
        }
        else if (type == ColumnType.DOUBLE) {
            long leftValue = leftSlice.getLong(leftOffset);
            long rightValue = Double.doubleToLongBits(rightCursor.getDouble());
            return leftValue == rightValue;
        }
        else if (type == ColumnType.BOOLEAN) {
            boolean leftValue = leftSlice.getByte(leftOffset) != 0;
            boolean rightValue = rightCursor.getBoolean();
            return leftValue == rightValue;
        }
        else {
            throw new IllegalArgumentException("Unsupported type " + type);
        }
    }

    public int hashCode(Slice slice, int offset)
    {
        if (type == ColumnType.LONG || type == ColumnType.DOUBLE) {
            return Longs.hashCode(slice.getLong(offset));
        }
        else if (type == ColumnType.BOOLEAN) {
            return Booleans.hashCode(slice.getByte(offset) != 0);
        }
        else {
            throw new IllegalArgumentException("Unsupported fixed width type " + type);
        }
    }

    public int compareTo(Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset)
    {
        if (type == ColumnType.LONG) {
            long leftValue = leftSlice.getLong(leftOffset);
            long rightValue = rightSlice.getLong(rightOffset);
            return Long.compare(leftValue, rightValue);
        }
        else if (type == ColumnType.DOUBLE) {
            double leftValue = leftSlice.getDouble(leftOffset);
            double rightValue = rightSlice.getDouble(rightOffset);
            return Double.compare(leftValue, rightValue);
        }
        else if (type == ColumnType.BOOLEAN) {
            boolean leftValue = leftSlice.getByte(leftOffset) != 0;
            boolean rightValue = rightSlice.getByte(rightOffset) != 0;
            return Boolean.compare(leftValue, rightValue);
        }
        else {
            throw new IllegalArgumentException("Unsupported type " + type);
        }
    }

    public void appendTo(Slice slice, int offset, BlockBuilder blockBuilder)
    {
        if (type == ColumnType.LONG) {
            long value = slice.getLong(offset);
            blockBuilder.append(value);
        }
        else if (type == ColumnType.DOUBLE) {
            double value = slice.getDouble(offset);
            blockBuilder.append(value);
        }
        else if (type == ColumnType.BOOLEAN) {
            boolean value = slice.getByte(offset) != 0;
            blockBuilder.append(value);
        }
        else {
            throw new IllegalArgumentException("Unsupported type " + type);
        }
    }

    @Override
    public void appendTo(Slice slice, int offset, SliceOutput sliceOutput)
    {
        sliceOutput.writeBytes(slice, offset, fixedSize);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FixedWidthType type = (FixedWidthType) o;

        if (!this.type.equals(type.type)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return type.hashCode();
    }

    @Override
    public String toString()
    {
        return getName();
    }
}
