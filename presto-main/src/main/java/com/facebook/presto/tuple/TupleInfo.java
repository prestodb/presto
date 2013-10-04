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

import com.facebook.presto.spi.ColumnType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import java.util.Map;

import static com.facebook.presto.tuple.TupleInfo.Type.BOOLEAN;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.util.Arrays.asList;

/**
 * Tuple layout for a fixed width value is:
 * <pre>
 *     is_null (byte)
 *     fixed_width_value
 * </pre>
 * <p/>
 * Tuple layout for a variable width value is:
 * <pre>
 *     is_null (byte)
 *     length (4 bytes)
 *     variable_width_value
 * </pre>
 * <p/>
 * Note: the null flag for each field is independent of the value of the field.  Specifically
 * this api will allow you to read and write the value of fields with the null flag set.
 */
public class TupleInfo
{
    public static final TupleInfo SINGLE_BOOLEAN = new TupleInfo(BOOLEAN);
    public static final TupleInfo SINGLE_LONG = new TupleInfo(FIXED_INT_64);
    public static final TupleInfo SINGLE_VARBINARY = new TupleInfo(VARIABLE_BINARY);
    public static final TupleInfo SINGLE_DOUBLE = new TupleInfo(DOUBLE);

    public enum Type
            implements Comparable<Type>
    {
        FIXED_INT_64(SIZE_OF_LONG, "bigint"),
        VARIABLE_BINARY(-1, "varchar"),
        DOUBLE(SIZE_OF_DOUBLE, "double"),
        BOOLEAN(SIZE_OF_BYTE, "boolean");

        private static final Map<String, Type> NAMES = uniqueIndex(asList(values()), nameGetter());

        private final int size;
        private final String name;

        Type(int size, String name)
        {
            this.size = size;
            this.name = name;
        }

        public int getSize()
        {
            checkState(isFixedSize(), "Can't get size of variable length value");
            return size;
        }

        public boolean isFixedSize()
        {
            return size != -1;
        }

        @JsonValue
        public String getName()
        {
            return name;
        }

        public ColumnType toColumnType()
        {
            switch (this) {
                case BOOLEAN:
                    return ColumnType.BOOLEAN;
                case FIXED_INT_64:
                    return ColumnType.LONG;
                case DOUBLE:
                    return ColumnType.DOUBLE;
                case VARIABLE_BINARY:
                    return ColumnType.STRING;
                default:
                    throw new IllegalStateException("Unknown type " + this);
            }
        }

        public static Type fromColumnType(ColumnType type)
        {
            switch (type) {
                case BOOLEAN:
                    return BOOLEAN;
                case DOUBLE:
                    return DOUBLE;
                case LONG:
                    return FIXED_INT_64;
                case STRING:
                    return VARIABLE_BINARY;
                default:
                    throw new IllegalStateException("Unknown type " + type);
            }
        }

        @JsonCreator
        public static Type fromName(String name)
        {
            checkNotNull(name, "name is null");
            Type encoding = NAMES.get(name);
            checkArgument(encoding != null, "Invalid type name: %s", name);
            return encoding;
        }

        public static Function<Type, String> nameGetter()
        {
            return new Function<Type, String>()
            {
                @Override
                public String apply(Type type)
                {
                    return type.getName();
                }
            };
        }
    }

    private final Type type;

    @JsonCreator
    public TupleInfo(Type tupleType)
    {
        this.type = checkNotNull(tupleType, "tupleType is null");
    }

    @JsonValue
    public Type getType()
    {
        return type;
    }

    public int getFixedSize()
    {
        return type.getSize() + 1;
    }

    public int size(Slice slice, int offset)
    {
        if (type.isFixedSize()) {
            return type.getSize() + SIZE_OF_BYTE;
        }
        // todo check uncompressed block for proper algorithm
        return slice.getInt(offset + SIZE_OF_BYTE);
    }

    /**
     * Extract the byte length of the Tuple Slice at the head of sliceInput
     * (Does not have any side effects on sliceInput position)
     */
    public int size(SliceInput sliceInput)
    {
        if (type.isFixedSize()) {
            return type.getSize();
        }

        // length of the tuple is located in the "last" fixed-width slot
        // this makes variable length column size easy to calculate
        int originalPosition = sliceInput.position();
        sliceInput.skipBytes(1);
        int tupleSize = sliceInput.readInt();
        sliceInput.setPosition(originalPosition);
        return tupleSize;
    }

    public boolean getBoolean(Slice slice, int offset)
    {
        checkState(type == BOOLEAN, "Expected BOOLEAN, but is %s", type);

        return slice.getByte(offset + SIZE_OF_BYTE) != 0;
    }

    /**
     * Sets the tuple at the specified offset to the specified boolean value.
     * <p/>
     * Note: this DOES NOT modify the null flag of this tuple.
     */
    public void setBoolean(Slice slice, int offset, boolean value)
    {
        checkState(type == BOOLEAN, "Expected BOOLEAN, but is %s", type);

        slice.setByte(offset + SIZE_OF_BYTE, value ? 1 : 0);
    }

    public long getLong(Slice slice, int offset)
    {
        checkState(type == FIXED_INT_64, "Expected FIXED_INT_64, but is %s", type);

        return slice.getLong(offset + SIZE_OF_BYTE);
    }

    /**
     * Sets the tuple at the specified offset to the specified long value.
     * <p/>
     * Note: this DOES NOT modify the null flag fo this tuple.
     */
    public void setLong(Slice slice, int offset, long value)
    {
        checkState(type == FIXED_INT_64, "Expected FIXED_INT_64, but is %s", type);

        slice.setLong(offset + SIZE_OF_BYTE, value);
    }

    public double getDouble(Slice slice, int offset)
    {
        checkState(type == DOUBLE, "Expected DOUBLE, but is %s", type);

        return slice.getDouble(offset + SIZE_OF_BYTE);
    }

    /**
     * Sets the tuple at the specified offset to the specified double value.
     * <p/>
     * Note: this DOES NOT modify the null flag fo this tuple.
     */
    public void setDouble(Slice slice, int offset, double value)
    {
        checkState(type == DOUBLE, "Expected DOUBLE, but is %s", type);

        slice.setDouble(offset + SIZE_OF_BYTE, value);
    }

    public Slice getSlice(Slice slice, int offset)
    {
        checkState(type == VARIABLE_BINARY, "Expected VARIABLE_BINARY, but is %s", type);

        int size = slice.getInt(offset + SIZE_OF_BYTE);
        return slice.slice(offset + SIZE_OF_INT + SIZE_OF_BYTE, size - SIZE_OF_INT - SIZE_OF_BYTE);
    }

    public boolean isNull(Slice slice, int offset)
    {
        return slice.getByte(offset) != 0;
    }

    /**
     * Marks the tuple at the specified offset as null.
     * <p/>
     * Note: this DOES NOT clear the current value of the tuple.
     */
    public void setNull(Slice slice, int offset)
    {
        slice.setByte(offset, 1);
    }

    /**
     * Marks the tuple at the specified offset as not null.
     * <p/>
     * Note this DOES NOT clear the current value of the tuple.
     */
    public void setNotNull(Slice slice, int offset)
    {
        slice.setByte(offset, 0);
    }

    /**
     * Extracts the Slice representation of a Tuple with this TupleInfo format from the head of a larger Slice.
     */
    public Slice extractTupleSlice(SliceInput sliceInput)
    {
        int tupleSliceSize = size(sliceInput);
        return sliceInput.readSlice(tupleSliceSize);
    }

    public Builder builder(SliceOutput sliceOutput)
    {
        return new Builder(sliceOutput);
    }

    public Builder builder()
    {
        return new Builder(new DynamicSliceOutput(0));
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

        TupleInfo tupleInfo = (TupleInfo) o;

        return type.equals(tupleInfo.type);
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

    public class Builder
    {
        private final SliceOutput sliceOutput;

        public Builder(SliceOutput sliceOutput)
        {
            this.sliceOutput = sliceOutput;
        }

        public Builder append(boolean value)
        {
            checkState(TupleInfo.this.type == BOOLEAN, "Cannot append boolean to type %s", TupleInfo.this.type);

            sliceOutput.writeByte(0);
            sliceOutput.writeByte(value ? 1 : 0);

            return this;
        }

        public Builder append(long value)
        {
            checkState(TupleInfo.this.type == FIXED_INT_64, "Cannot append long to type %s", TupleInfo.this.type);

            sliceOutput.writeByte(0);
            sliceOutput.writeLong(value);

            return this;
        }

        public Builder append(double value)
        {
            checkState(TupleInfo.this.type == DOUBLE, "Cannot append double to type %s", TupleInfo.this.type);

            sliceOutput.writeByte(0);
            sliceOutput.writeDouble(value);

            return this;
        }

        public Builder append(String value)
        {
            return append(Slices.copiedBuffer(value, Charsets.UTF_8));
        }

        public Builder append(Slice value)
        {
            return append(value, 0, value.length());
        }

        public Builder append(Slice value, int offset, int length)
        {
            checkState(TupleInfo.this.type == VARIABLE_BINARY, "Cannot append binary to type %s", TupleInfo.this.type);

            sliceOutput.writeByte(0);
            sliceOutput.writeInt(length + SIZE_OF_BYTE + SIZE_OF_INT);
            sliceOutput.writeBytes(value, offset, length);

            return this;
        }

        public Builder appendNull()
        {
            sliceOutput.writeByte(1);
            switch (type) {
                case FIXED_INT_64:
                    sliceOutput.writeLong(0);
                    break;
                case VARIABLE_BINARY:
                    sliceOutput.writeInt(SIZE_OF_BYTE + SIZE_OF_INT);
                    break;
                case DOUBLE:
                    sliceOutput.writeDouble(0);
                    break;
                case BOOLEAN:
                    sliceOutput.writeByte(0);
                    break;
                default:
                    throw new IllegalStateException("Unsupported type: " + type);
            }

            return this;
        }

        public Builder append(TupleReadable tuple)
        {
            checkArgument(type == tuple.getTupleInfo().getType(), "Type (%s) does not match tuple type (%s)", type, tuple.getTupleInfo().getType());

            if (tuple.isNull()) {
                appendNull();
            }
            else {
                switch (type) {
                    case BOOLEAN:
                        append(tuple.getBoolean());
                        break;
                    case FIXED_INT_64:
                        append(tuple.getLong());
                        break;
                    case DOUBLE:
                        append(tuple.getDouble());
                        break;
                    case VARIABLE_BINARY:
                        append(tuple.getSlice());
                        break;
                    default:
                        throw new IllegalStateException("Type not yet supported: " + type);
                }
            }

            return this;
        }

        public Tuple build()
        {
            return new Tuple(sliceOutput.slice(), TupleInfo.this);
        }
    }
}
