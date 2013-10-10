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
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Charsets;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;

public class VariableWidthTypeInfo
        implements TypeInfo
{
    private final Type type;

    @JsonCreator
    public VariableWidthTypeInfo(Type type)
    {
        this.type = checkNotNull(type, "type is null");
        checkArgument(!type.isFixedSize(), "type %s is not a variable width type", type);
    }

    @Override
    @JsonValue
    public Type getType()
    {
        return type;
    }

    @Override
    public Object getObjectValue(Slice slice, int offset)
    {
        checkState(type == VARIABLE_BINARY, "Expected VARIABLE_BINARY, but is %s", type);
        return slice.toString(offset + SIZE_OF_INT, getValueSize(slice, offset), Charsets.UTF_8);
    }

    public int getLength(Slice slice, int offset)
    {
        return getValueSize(slice, offset) + SIZE_OF_INT;
    }

    /**
     * Extract the byte length of the Tuple Slice at the head of sliceInput
     * (Does not have any side effects on sliceInput position)
     */
    public int getLength(SliceInput sliceInput)
    {
        int originalPosition = sliceInput.position();
        int tupleSize = sliceInput.readInt();
        sliceInput.setPosition(originalPosition);
        return tupleSize + SIZE_OF_INT;
    }

    private int getValueSize(Slice slice, int offset)
    {
        return slice.getInt(offset);
    }

    public Slice getSlice(Slice slice, int offset)
    {
        checkState(type == VARIABLE_BINARY, "Expected VARIABLE_BINARY, but is %s", type);
        return slice.slice(offset + SIZE_OF_INT, getValueSize(slice, offset));
    }

    @Override
    public boolean equals(Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset)
    {
        int leftLength = getValueSize(leftSlice, leftOffset);
        int rightLength = getValueSize(rightSlice, rightOffset);
        return leftSlice.equals(leftOffset + SIZE_OF_INT, leftLength, rightSlice, rightOffset + SIZE_OF_INT, rightLength);
    }

    @Override
    public boolean equals(Slice leftSlice, int leftOffset, BlockCursor rightCursor)
    {
        int leftLength = getValueSize(leftSlice, leftOffset);
        Slice rightSlice = rightCursor.getSlice();
        return leftSlice.equals(leftOffset + SIZE_OF_INT, leftLength, rightSlice, 0, rightSlice.length());
    }

    @Override
    public int hashCode(Slice slice, int offset)
    {
        int length = getValueSize(slice, offset);
        return slice.hashCode(offset + SIZE_OF_INT, length);
    }

    @Override
    public int compareTo(Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset)
    {
        int leftLength = getValueSize(leftSlice, leftOffset);
        int rightLength = getValueSize(rightSlice, rightOffset);
        return leftSlice.compareTo(leftOffset + SIZE_OF_INT, leftLength, rightSlice, rightOffset + SIZE_OF_INT, rightLength);
    }

    @Override
    public void appendTo(Slice slice, int offset, BlockBuilder blockBuilder)
    {
        int length = getValueSize(slice, offset);
        blockBuilder.append(slice, offset + SIZE_OF_INT, length);
    }

    @Override
    public void appendTo(Slice slice, int offset, SliceOutput sliceOutput)
    {
        // copy full value including length
        int length = getLength(slice, offset);
        sliceOutput.writeBytes(slice, offset, length);
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

        VariableWidthTypeInfo tupleInfo = (VariableWidthTypeInfo) o;

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
