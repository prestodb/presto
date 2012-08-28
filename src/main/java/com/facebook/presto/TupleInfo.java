package com.facebook.presto;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.SizeOf.SIZE_OF_SHORT;
import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;

/**
 * Tuple layout is:
 * <pre>
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
    public enum Type
    {
        FIXED_INT_64(8),
        VARIABLE_BINARY(-1);

        private final int size;

        private Type(int size)
        {
            this.size = size;
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

    public TupleInfo(List<Type> types)
    {
        Preconditions.checkNotNull(types, "types is null");
        Preconditions.checkArgument(!types.isEmpty(), "types is empty");

        this.types = ImmutableList.copyOf(types);

        int[] offsets = new int[types.size() + 1];

        int currentOffset = 0;

        // process fixed-length fields first
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
                    currentOffset += SIZE_OF_SHORT; // we use a short to encode the offset of a var length field

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
        int variablePartOffset = 0;
        boolean isFirst = true;
        for (TupleInfo.Type type : getTypes()) {
            if (!type.isFixedSize()) {
                if (!isFirst) { // skip offset field for first variable length field
                    variablePartOffset += SizeOf.SIZE_OF_SHORT;
                }

                isFirst = false;
            }
            else {
                variablePartOffset += type.getSize();
            }
        }
        variablePartOffset += SizeOf.SIZE_OF_SHORT; // total tuple size field

        this.variablePartOffset = variablePartOffset;
    }

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
        return slice.getShort(offset + getOffset(types.size()));
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
            end = slice.getShort(offset + getOffset(secondVariableLengthField));
        }
        else {
            start = slice.getShort(offset + getOffset(field));
            end = slice.getShort(offset + getOffset(field) + SIZE_OF_SHORT);
        }

        // this works because positions of variable length fields are laid out in the same order as the actual data
        return slice.slice(offset + start, end - start);
    }

    public boolean equals(int field, Slice block, int tupleOffset, Slice value)
    {
        int start;
        int end;
        if (field == firstVariableLengthField) {
            start = variablePartOffset;
            end = block.getShort(tupleOffset + getOffset(secondVariableLengthField));
        }
        else {
            start = block.getShort(tupleOffset + getOffset(field));
            end = block.getShort(tupleOffset + getOffset(field) + SIZE_OF_SHORT);
        }

        return value.equals(0, value.length(), block, tupleOffset + start, end - start);
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
        return Objects.toStringHelper(this)
                .add("size", size)
                .add("types", types)
                .add("offsets", offsets)
                .add("firstVariableLengthField", firstVariableLengthField)
                .add("secondVariableLengthField", secondVariableLengthField)
                .add("variableLengthFieldCount", variableLengthFieldCount)
                .add("variablePartOffset", variablePartOffset)
                .toString();
    }

    public class Builder
    {
        private final SliceOutput sliceOutput;
        private final List<Slice> variableLengthFields;

        private int currentField;

        public Builder(SliceOutput sliceOutput)
        {
            this.sliceOutput = sliceOutput;
            this.variableLengthFields = new ArrayList<>(variableLengthFieldCount);
        }

        public Builder append(long value)
        {
            checkState(TupleInfo.this.getTypes().get(currentField) == FIXED_INT_64, "Cannot append long. Current field (%s) is of type %s", currentField, TupleInfo.this.getTypes().get(currentField));

            sliceOutput.writeLong(value);
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

        public void append(Tuple tuple)
        {
            // TODO: optimization - single copy of block of fixed length fields

            int field = 0;
            for (TupleInfo.Type type : tuple.getTupleInfo().getTypes()) {
                switch (type) {
                    case FIXED_INT_64:
                        append(tuple.getLong(field));
                        break;
                    case VARIABLE_BINARY:
                        append(tuple.getSlice(field));
                        break;
                    default:
                        throw new IllegalStateException("Type not yet supported: " + type);
                }

                field++;
            }
        }

        public boolean isComplete()
        {
            return currentField == types.size();
        }

        public void finish()
        {
            Preconditions.checkState(currentField == types.size(), "Tuple is incomplete");

            // write offsets
            boolean isFirst = true;
            int offset = variablePartOffset;
            for (Slice field : variableLengthFields) {
                if (!isFirst) {
                    sliceOutput.writeShort(offset);
                }
                offset += field.length();
                isFirst = false;
            }

            if (!variableLengthFields.isEmpty()) {
                sliceOutput.writeShort(offset); // total tuple length
            }

            // write values
            for (Slice field : variableLengthFields) {
                sliceOutput.writeBytes(field);
            }

            currentField = 0;
            variableLengthFields.clear();
        }

        public Tuple build()
        {
            finish();
            return new Tuple(sliceOutput.slice(), TupleInfo.this);
        }
    }

}
