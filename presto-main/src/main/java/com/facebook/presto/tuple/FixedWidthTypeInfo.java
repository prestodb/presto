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
package com.facebook.presto.tuple;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Longs;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import static com.facebook.presto.tuple.TupleInfo.Type.BOOLEAN;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class FixedWidthTypeInfo
        implements TypeInfo
{
    private final Type type;

    @JsonCreator
    public FixedWidthTypeInfo(Type type)
    {
        this.type = checkNotNull(type, "type is null");
        checkArgument(type.isFixedSize(), "type %s is not a fixed width type", type);
    }

    @JsonValue
    public Type getType()
    {
        return type;
    }

    public int getSize()
    {
        return type.getSize();
    }

    public boolean getBoolean(Slice slice, int offset)
    {
        checkState(type == BOOLEAN, "Expected BOOLEAN, but is %s", type);

        return slice.getByte(offset) != 0;
    }

    public void setBoolean(Slice slice, int offset, boolean value)
    {
        checkState(type == BOOLEAN, "Expected BOOLEAN, but is %s", type);

        slice.setByte(offset, value ? 1 : 0);
    }

    public long getLong(Slice slice, int offset)
    {
        checkState(type == FIXED_INT_64, "Expected FIXED_INT_64, but is %s", type);

        return slice.getLong(offset);
    }

    public void setLong(Slice slice, int offset, long value)
    {
        checkState(type == FIXED_INT_64, "Expected FIXED_INT_64, but is %s", type);

        slice.setLong(offset, value);
    }

    public double getDouble(Slice slice, int offset)
    {
        checkState(type == DOUBLE, "Expected DOUBLE, but is %s", type);

        return slice.getDouble(offset);
    }

    public void setDouble(Slice slice, int offset, double value)
    {
        checkState(type == DOUBLE, "Expected DOUBLE, but is %s", type);

        slice.setDouble(offset, value);
    }

    public Slice getSlice(Slice slice, int offset)
    {
        checkState(type == VARIABLE_BINARY, "Expected VARIABLE_BINARY, but is %s", type);

        return slice.slice(offset, getSize());
    }

    public boolean equals(Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset)
    {
        if (type == Type.FIXED_INT_64 || type == Type.DOUBLE) {
            long leftValue = leftSlice.getLong(leftOffset);
            long rightValue = rightSlice.getLong(rightOffset);
            return leftValue == rightValue;
        }
        else if (type == Type.BOOLEAN) {
            boolean leftValue = leftSlice.getByte(leftOffset) != 0;
            boolean rightValue = rightSlice.getByte(rightOffset) != 0;
            return leftValue == rightValue;
        }
        else {
            throw new IllegalArgumentException("Unsupported type " + type);
        }
    }

    public boolean equals(Slice leftSlice, int leftOffset, TupleReadable rightTuple)
    {
        if (type == Type.FIXED_INT_64) {
            long leftValue = leftSlice.getLong(leftOffset);
            long rightValue = rightTuple.getLong();
            return leftValue == rightValue;
        }
        else if (type == Type.DOUBLE) {
            long leftValue = leftSlice.getLong(leftOffset);
            long rightValue = Double.doubleToLongBits(rightTuple.getDouble());
            return leftValue == rightValue;
        }
        else if (type == Type.BOOLEAN) {
            boolean leftValue = leftSlice.getByte(leftOffset) != 0;
            boolean rightValue = rightTuple.getBoolean();
            return leftValue == rightValue;
        }
        else {
            throw new IllegalArgumentException("Unsupported type " + type);
        }
    }

    public int hashCode(Slice slice, int offset)
    {
        if (type == Type.FIXED_INT_64 || type == Type.DOUBLE) {
            return Longs.hashCode(slice.getLong(offset));
        }
        else if (type == Type.BOOLEAN) {
            return Booleans.hashCode(slice.getByte(offset) != 0);
        }
        else {
            throw new IllegalArgumentException("Unsupported fixed width type " + type);
        }
    }

    public int compareTo(Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset)
    {
        if (type == Type.FIXED_INT_64) {
            long leftValue = leftSlice.getLong(leftOffset);
            long rightValue = rightSlice.getLong(rightOffset);
            return Long.compare(leftValue, rightValue);
        }
        else if (type == Type.DOUBLE) {
            double leftValue = leftSlice.getDouble(leftOffset);
            double rightValue = rightSlice.getDouble(rightOffset);
            return Double.compare(leftValue, rightValue);
        }
        else if (type == Type.BOOLEAN) {
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
        if (type == Type.FIXED_INT_64) {
            long value = slice.getLong(offset);
            blockBuilder.append(value);
        }
        else if (type == Type.DOUBLE) {
            double value = slice.getDouble(offset);
            blockBuilder.append(value);
        }
        else if (type == Type.BOOLEAN) {
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
        sliceOutput.writeBytes(slice, offset, type.getSize());
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

        FixedWidthTypeInfo tupleInfo = (FixedWidthTypeInfo) o;

        if (!type.equals(tupleInfo.type)) {
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
        return "TupleInfo{" + type + "}";
    }
}
