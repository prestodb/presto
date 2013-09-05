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
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.List;
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
 * Tuple layout is:
 * <pre>
 *     is_null_0_to_7
 *     is_null_8_to_15
 *     ...
 *     fixed_0
 *     fixed_1
 *     ...
 *     fixed_N
 *     var_off_1
 *     var_off_2
 *     ...
 *     var_off_N
 *     size
 *     var_0
 *     var_1
 *     ...
 *     var_N
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

        int getSize()
        {
            checkState(isFixedSize(), "Can't get size of variable length field");
            return size;
        }

        boolean isFixedSize()
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

    private final int size;

    private final List<Type> types;
    private final List<Integer> offsets; // Offset of either a fixed sized field, or the offset of a pointer to a variable length field
    private final int firstVariableLengthField;
    private final int secondVariableLengthField;
    private final int variableLengthFieldCount;
    private final int variablePartOffset;

    public TupleInfo(Type... types)
    {
        this(asList(types));
    }

    public TupleInfo(Iterable<Type> types)
    {
        this(ImmutableList.copyOf(types));
    }

    @JsonCreator
    public TupleInfo(List<Type> typeIterable)
    {
        checkNotNull(typeIterable, "typeIterable is null");
        // checkArgument(!typeIterable.isEmpty(), "types is empty");

        this.types = ImmutableList.copyOf(typeIterable);

        int[] offsets = new int[types.size() + 1];

        // calculate number of null bytes
        int nullBytes = ((types.size() - 1) >> 3) + 1;

        // process fixed-length fields first
        int currentOffset = nullBytes;
        for (int i = 0; i < types.size(); i++) {
            Type type = types.get(i);

            if (type.isFixedSize()) {
                offsets[i] = currentOffset;
                currentOffset += type.getSize();
            }
        }

        boolean hasVariableLengthFields = false;

        int firstVariableLengthField = -1;
        int secondVariableLengthField = -1;

        int variableLengthFieldCount = 0;
        // process variable length fields
        for (int i = 0; i < types.size(); i++) {
            Type type = types.get(i);

            if (!type.isFixedSize()) {
                ++variableLengthFieldCount;
                offsets[i] = currentOffset;
                if (hasVariableLengthFields) {
                    currentOffset += SIZE_OF_INT; // we use an int to encode the offset of a var length field

                    if (secondVariableLengthField == -1) {
                        secondVariableLengthField = i;
                    }
                }
                else {
                    firstVariableLengthField = i;
                }

                hasVariableLengthFields = true;
            }
        }

        if (secondVariableLengthField == -1) {
            secondVariableLengthField = types.size(); // use the length field
        }

        offsets[offsets.length - 1] = currentOffset;

        if (hasVariableLengthFields) {
            size = -1;
        }
        else {
            size = currentOffset;
        }

        this.firstVariableLengthField = firstVariableLengthField;
        this.secondVariableLengthField = secondVariableLengthField;
        this.variableLengthFieldCount = variableLengthFieldCount;

        this.offsets = ImmutableList.copyOf(Ints.asList(offsets));

        // compute offset of variable sized part
        int variablePartOffset = nullBytes;
        boolean isFirst = true;
        for (TupleInfo.Type type : getTypes()) {
            if (!type.isFixedSize()) {
                if (!isFirst) {
                    // skip offset field for first variable length field
                    variablePartOffset += SizeOf.SIZE_OF_INT;
                }

                isFirst = false;
            }
            else {
                variablePartOffset += type.getSize();
            }
        }
        variablePartOffset += SizeOf.SIZE_OF_INT; // total tuple size field

        this.variablePartOffset = variablePartOffset;
    }

    @JsonValue
    public List<Type> getTypes()
    {
        return types;
    }

    public int getFieldCount()
    {
        return types.size();
    }

    public int getFixedSize()
    {
        return size;
    }

    public int size(Slice slice)
    {
        return size(slice, 0);
    }

    public int size(Slice slice, int offset)
    {
        if (size != -1) {
            return size;
        }

        // length of the tuple is located in the "last" fixed-width slot
        // this makes variable length column size easy to calculate
        return slice.getInt(offset + getTupleSizeOffset());
    }

    /**
     * Extract the byte length of the Tuple Slice at the head of sliceInput
     * (Does not have any side effects on sliceInput position)
     */
    public int size(SliceInput sliceInput)
    {
        if (size != -1) {
            return size;
        }

        // length of the tuple is located in the "last" fixed-width slot
        // this makes variable length column size easy to calculate
        int originalPosition = sliceInput.position();
        sliceInput.skipBytes(getTupleSizeOffset());
        int tupleSize = sliceInput.readInt();
        sliceInput.setPosition(originalPosition);
        return tupleSize;
    }

    public boolean getBoolean(Slice slice, int field)
    {
        return getBoolean(slice, 0, field);
    }

    public boolean getBoolean(Slice slice, int offset, int field)
    {
        checkState(types.get(field) == BOOLEAN, "Expected BOOLEAN, but is %s", types.get(field));

        return slice.getByte(offset + getOffset(field)) != 0;
    }

    public void setBoolean(Slice slice, int field, boolean value)
    {
        setBoolean(slice, 0, field, value);
    }

    /**
     * Sets the specified field to the specified boolean value.
     * <p/>
     * Note: this DOES NOT modify the null flag of this field.
     */
    public void setBoolean(Slice slice, int offset, int field, boolean value)
    {
        checkState(types.get(field) == BOOLEAN, "Expected BOOLEAN, but is %s", types.get(field));

        slice.setByte(offset + getOffset(field), value ? 1 : 0);
    }

    public long getLong(Slice slice, int field)
    {
        return getLong(slice, 0, field);
    }

    public long getLong(Slice slice, int offset, int field)
    {
        checkState(types.get(field) == FIXED_INT_64, "Expected FIXED_INT_64, but is %s", types.get(field));

        return slice.getLong(offset + getOffset(field));
    }

    public void setLong(Slice slice, int field, long value)
    {
        setLong(slice, 0, field, value);
    }

    /**
     * Sets the specified field to the specified long value.
     * <p/>
     * Note: this DOES NOT modify the null flag fo this field.
     */
    public void setLong(Slice slice, int offset, int field, long value)
    {
        checkState(types.get(field) == FIXED_INT_64, "Expected FIXED_INT_64, but is %s", types.get(field));

        slice.setLong(offset + getOffset(field), value);
    }

    public double getDouble(Slice slice, int field)
    {
        return getDouble(slice, 0, field);
    }

    public double getDouble(Slice slice, int offset, int field)
    {
        checkState(types.get(field) == DOUBLE, "Expected DOUBLE, but is %s", types.get(field));

        return slice.getDouble(offset + getOffset(field));
    }

    public void setDouble(Slice slice, int field, double value)
    {
        setDouble(slice, 0, field, value);
    }

    /**
     * Sets the specified field to the specified double value.
     * <p/>
     * Note: this DOES NOT modify the null flag fo this field.
     */
    public void setDouble(Slice slice, int offset, int field, double value)
    {
        checkState(types.get(field) == DOUBLE, "Expected DOUBLE, but is %s", types.get(field));

        slice.setDouble(offset + getOffset(field), value);
    }

    public Slice getSlice(Slice slice, int field)
    {
        return getSlice(slice, 0, field);
    }

    public Slice getSlice(Slice slice, int offset, int field)
    {
        checkState(types.get(field) == VARIABLE_BINARY, "Expected VARIABLE_BINARY, but is %s", types.get(field));

        int start;
        int end;
        if (field == firstVariableLengthField) {
            start = variablePartOffset;
            end = slice.getInt(offset + getOffset(secondVariableLengthField));
        }
        else {
            start = slice.getInt(offset + getOffset(field));
            end = slice.getInt(offset + getOffset(field) + SIZE_OF_INT);
        }

        // this works because positions of variable length fields are laid out in the same order as the actual data
        return slice.slice(offset + start, end - start);
    }

    public boolean isNull(Slice slice, int field)
    {
        return isNull(slice, 0, field);
    }

    public boolean isNull(Slice slice, int offset, int field)
    {
        int index = field >> 3;
        int bit = field & 0b111;
        int bitMask = 1 << bit;
        return (slice.getByte(offset + index) & bitMask) != 0;
    }

    /**
     * Marks the specified field as null.
     * <p/>
     * Note: this DOES NOT clear the current value of the field.
     */
    public void setNull(Slice slice, int offset, int field)
    {
        int index = field >> 3;
        int bit = field & 0b111;
        int bitMask = 1 << bit;
        slice.setByte(index + offset, slice.getByte(index + offset) | bitMask);
    }

    public void setNotNull(Slice slice, int field)
    {
        setNotNull(slice, 0, field);
    }

    /**
     * Marks the specified field as not null.
     * <p/>
     * Note this DOES NOT clear the current value of the field.
     */
    public void setNotNull(Slice slice, int offset, int field)
    {
        int index = field >> 3;
        int bit = field & 0b111;
        int bitMask = ~(1 << bit);
        slice.setByte(index + offset, slice.getByte(index + offset) & bitMask);
    }

    /**
     * Extracts the Slice representation of a Tuple with this TupleInfo format from the head of a larger Slice.
     */
    public Slice extractTupleSlice(SliceInput sliceInput)
    {
        int tupleSliceSize = size(sliceInput);
        return sliceInput.readSlice(tupleSliceSize);
    }

    /**
     * Extracts a Tuple with this TupleInfo format from the head of a Slice.
     */
    public Tuple extractTuple(SliceInput sliceInput)
    {
        return new Tuple(extractTupleSlice(sliceInput), this);
    }

    public boolean equals(int field, Slice block, int tupleOffset, Slice value)
    {
        int start;
        int end;
        if (field == firstVariableLengthField) {
            start = variablePartOffset;
            end = block.getInt(tupleOffset + getOffset(secondVariableLengthField));
        }
        else {
            start = block.getInt(tupleOffset + getOffset(field));
            end = block.getInt(tupleOffset + getOffset(field) + SIZE_OF_INT);
        }

        return value.equals(0, value.length(), block, tupleOffset + start, end - start);
    }

    public int getTupleSizeOffset()
    {
        return getOffset(types.size());
    }

    private int getOffset(int field)
    {
        checkArgument(field != firstVariableLengthField, "Cannot get offset for first variable length field");
        return offsets.get(field);
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

        return types.equals(tupleInfo.types);

    }

    @Override
    public int hashCode()
    {
        return types.hashCode();
    }

    @Override
    public String toString()
    {
        return "TupleInfo{" + Joiner.on(",").join(types) + "}";
    }

    public class Builder
    {
        private final SliceOutput sliceOutput;
        private final List<Slice> variableLengthFields;
        private final Slice fixedBuffer;

        private int currentField;

        public Builder(SliceOutput sliceOutput)
        {
            this.sliceOutput = sliceOutput;
            this.variableLengthFields = new ArrayList<>(variableLengthFieldCount);
            fixedBuffer = Slices.allocate(size < 0 ? getOffset(secondVariableLengthField) : size);
        }

        public Builder append(boolean value)
        {
            checkState(TupleInfo.this.getTypes().get(currentField) == BOOLEAN, "Cannot append boolean. Current field (%s) is of type %s", currentField, TupleInfo.this.getTypes().get(currentField));

            fixedBuffer.setByte(getOffset(currentField), value ? 1 : 0);
            currentField++;

            return this;
        }

        public Builder append(long value)
        {
            checkState(TupleInfo.this.getTypes().get(currentField) == FIXED_INT_64, "Cannot append long. Current field (%s) is of type %s", currentField, TupleInfo.this.getTypes().get(currentField));

            fixedBuffer.setLong(getOffset(currentField), value);
            currentField++;

            return this;
        }

        public Builder append(double value)
        {
            checkState(TupleInfo.this.getTypes().get(currentField) == DOUBLE, "Cannot append double. Current field (%s) is of type %s", currentField, TupleInfo.this.getTypes().get(currentField));

            fixedBuffer.setDouble(getOffset(currentField), value);
            currentField++;

            return this;
        }

        public Builder append(String value)
        {
            return append(Slices.copiedBuffer(value, Charsets.UTF_8));
        }

        public Builder append(Slice value)
        {
            checkState(TupleInfo.this.getTypes().get(currentField) == VARIABLE_BINARY, "Cannot append binary. Current field (%s) is of type %s", currentField, TupleInfo.this.getTypes().get(currentField));

            variableLengthFields.add(value);
            currentField++;

            return this;
        }

        public Builder appendNull()
        {
            int index = currentField >> 3;
            int bit = currentField & 0b111;
            int bitMask = 1 << bit;
            fixedBuffer.setByte(index, fixedBuffer.getByte(index) | bitMask);

            if (TupleInfo.this.getTypes().get(currentField) == VARIABLE_BINARY) {
                variableLengthFields.add(null);
            }
            currentField++;

            return this;
        }

        public Builder append(TupleReadable tuple)
        {
            // TODO: optimization - single copy of block of fixed length fields

            for (int field = 0; field < tuple.getTupleInfo().getFieldCount(); field++) {
                append(tuple, field);
            }
            return this;
        }

        public Builder append(TupleReadable tuple, int index)
        {
            Type type = TupleInfo.this.getTypes().get(currentField);
            checkArgument(type == tuple.getTupleInfo().getTypes().get(index), "Current field (%s) type (%s) does not match tuple field (%s) type (%s)",
                    currentField, type, index, tuple.getTupleInfo().getTypes().get(index));

            if (tuple.isNull(index)) {
                appendNull();
            }
            else {
                switch (type) {
                    case BOOLEAN:
                        append(tuple.getBoolean(index));
                        break;
                    case FIXED_INT_64:
                        append(tuple.getLong(index));
                        break;
                    case DOUBLE:
                        append(tuple.getDouble(index));
                        break;
                    case VARIABLE_BINARY:
                        append(tuple.getSlice(index));
                        break;
                    default:
                        throw new IllegalStateException("Type not yet supported: " + type);
                }
            }

            return this;
        }

        public boolean isComplete()
        {
            return currentField == types.size();
        }

        public boolean isPartial()
        {
            return (currentField > 0) && (!isComplete());
        }

        public void finish()
        {
            checkState(isComplete(), "Tuple is incomplete");

            // write fixed part
            sliceOutput.writeBytes(fixedBuffer);

            // write offsets
            boolean isFirst = true;
            int offset = variablePartOffset;
            for (Slice field : variableLengthFields) {
                if (!isFirst) {
                    sliceOutput.writeInt(offset);
                }
                if (field != null) {
                    offset += field.length();
                }
                isFirst = false;
            }

            if (!variableLengthFields.isEmpty()) {
                sliceOutput.writeInt(offset); // total tuple length
            }

            // write values
            for (Slice field : variableLengthFields) {
                if (field != null) {
                    sliceOutput.writeBytes(field);
                }
            }

            currentField = 0;
            variableLengthFields.clear();
            fixedBuffer.clear();
        }

        public Tuple build()
        {
            finish();
            return new Tuple(sliceOutput.slice(), TupleInfo.this);
        }
    }
}
