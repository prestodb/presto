package com.facebook.presto.tuple;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.slice.DynamicSliceOutput;
import com.facebook.presto.slice.SizeOf;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import com.facebook.presto.slice.Slices;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.slice.SizeOf.SIZE_OF_DOUBLE;
import static com.facebook.presto.slice.SizeOf.SIZE_OF_INT;
import static com.facebook.presto.slice.SizeOf.SIZE_OF_LONG;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
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
 */
public class TupleInfo
{
    public static final TupleInfo SINGLE_LONG = new TupleInfo(FIXED_INT_64);
    public static final TupleInfo SINGLE_VARBINARY = new TupleInfo(VARIABLE_BINARY);
    public static final TupleInfo SINGLE_DOUBLE = new TupleInfo(DOUBLE);

    public enum Type
    {
        FIXED_INT_64(SIZE_OF_LONG, "long")
                {
                    @Override
                    public void convert(String value, BlockBuilder blockBuilder)
                    {
                        blockBuilder.append(Long.parseLong(value));
                    }

                    @Override
                    public void convert(String value, TupleInfo.Builder tupleBuilder)
                    {
                        tupleBuilder.append(Long.parseLong(value));
                    }
                },
        VARIABLE_BINARY(-1, "string")
                {
                    @Override
                    public void convert(String value, BlockBuilder blockBuilder)
                    {
                        blockBuilder.append(value.getBytes(Charsets.UTF_8));
                    }

                    @Override
                    public void convert(String value, TupleInfo.Builder tupleBuilder)
                    {
                        tupleBuilder.append(Slices.wrappedBuffer(value.getBytes(Charsets.UTF_8)));
                    }
                },
        DOUBLE(SIZE_OF_DOUBLE, "double")
                {
                    @Override
                    public void convert(String value, BlockBuilder blockBuilder)
                    {
                        blockBuilder.append(Double.parseDouble(value));
                    }

                    @Override
                    public void convert(String value, TupleInfo.Builder tupleBuilder)
                    {
                        tupleBuilder.append(Double.parseDouble(value));
                    }
                };

        private static final Map<String, Type> NAME_MAP;
        static {
            ImmutableMap.Builder<String, Type> builder = ImmutableMap.builder();
            for (Type encoding : Type.values()) {
                builder.put(encoding.getName(), encoding);
            }
            NAME_MAP = builder.build();
        }

        private final int size;
        private final String name;

        private Type(int size, String name)
        {
            this.size = size;
            this.name = name;
        }

        int getSize()
        {
            Preconditions.checkState(isFixedSize(), "Can't get size of variable length field");
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

        public abstract void convert(String value, BlockBuilder blockBuilder);

        public abstract void convert(String value, Builder tupleBuilder);

        @JsonCreator
        public static Type fromName(String name)
        {
            checkNotNull(name, "name is null");
            Type encoding = NAME_MAP.get(name);
            checkArgument(encoding != null, "Invalid type name: %s", name);
            return encoding;
        }
    }

    private final int size;

    private final List<Type> types;
    private final List<Integer> offsets;
    private final int firstVariableLengthField;
    private final int secondVariableLengthField;
    private final int variableLengthFieldCount;
    private final int variablePartOffset;

    public TupleInfo(Type... types)
    {
        this(asList(types));
    }

    @JsonCreator
    public TupleInfo(@JsonProperty("types") Iterable<Type> typeIterable)
    {
        checkNotNull(typeIterable, "typeIterable is null");
//        Preconditions.checkArgument(!types.isEmpty(), "types is empty");

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

    @JsonProperty
    public List<Type> getTypes()
    {
        return types;
    }

    public int getFieldCount()
    {
        return types.size();
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
        return slice.getInt(offset + getTupleSizeOffset()) & 0x7F_FF_FF_FF;
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

    public long getLong(Slice slice, int field)
    {
        return getLong(slice, 0, field);
    }

    public long getLong(Slice slice, int offset, int field)
    {
        checkState(types.get(field) == FIXED_INT_64, "Expected FIXED_INT_64");

        return slice.getLong(offset + getOffset(field));
    }

    public double getDouble(Slice slice, int field)
    {
        return getDouble(slice, 0, field);
    }

    public double getDouble(Slice slice, int offset, int field)
    {
        checkState(types.get(field) == DOUBLE, "Expected DOUBLE");

        return slice.getDouble(offset + getOffset(field));
    }

    public Slice getSlice(Slice slice, int field)
    {
        return getSlice(slice, 0, field);
    }

    public Slice getSlice(Slice slice, int offset, int field)
    {
        checkState(types.get(field) == VARIABLE_BINARY, "Expected VARIABLE_BINARY");

        int start;
        int end;
        if (field == firstVariableLengthField) {
            start = variablePartOffset;
            end = slice.getInt(offset + getOffset(secondVariableLengthField)) & 0x7F_FF_FF_FF;
        }
        else {
            start = slice.getInt(offset + getOffset(field)) & 0x7F_FF_FF_FF;
            end = slice.getInt(offset + getOffset(field) + SIZE_OF_INT) & 0x7F_FF_FF_FF;
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
            end = block.getInt(tupleOffset + getOffset(secondVariableLengthField)) & 0x7F_FF_FF_FF;
        }
        else {
            start = block.getInt(tupleOffset + getOffset(field)) & 0x7F_FF_FF_FF;
            end = block.getInt(tupleOffset + getOffset(field) + SIZE_OF_INT) & 0x7F_FF_FF_FF;
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

        if (!types.equals(tupleInfo.types)) {
            return false;
        }

        return true;
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

        public Builder append(Tuple tuple)
        {
            // TODO: optimization - single copy of block of fixed length fields

            int field = 0;
            for (TupleInfo.Type type : tuple.getTupleInfo().getTypes()) {
                if (tuple.isNull(field)) {
                    appendNull();
                }
                else {
                    switch (type) {
                        case FIXED_INT_64:
                            append(tuple.getLong(field));
                            break;
                        case DOUBLE:
                            append(tuple.getDouble(field));
                            break;
                        case VARIABLE_BINARY:
                            append(tuple.getSlice(field));
                            break;
                        default:
                            throw new IllegalStateException("Type not yet supported: " + type);
                    }
                }
                field++;
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
