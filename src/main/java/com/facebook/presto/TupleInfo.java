package com.facebook.presto;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.util.List;

import static com.facebook.presto.SizeOf.SIZE_OF_SHORT;
import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;

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

        // process variable length fields
        for (int i = 0; i < types.size(); i++) {
            Type type = types.get(i);

            if (!type.isFixedSize()) {
                hasVariableLengthFields = true;
                offsets[i] = currentOffset;
                currentOffset += SIZE_OF_SHORT; // we use a short to encode the offset of a var length field
            }
        }

        offsets[offsets.length - 1] = currentOffset;

        if (hasVariableLengthFields) {
            size = -1;
        }
        else {
            size = currentOffset;
        }

        this.offsets = ImmutableList.copyOf(Ints.asList(offsets));
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public int size(Slice slice)
    {
        if (size != -1) {
            return size;
        }

        // length of the tuple is located in the "last" fixed-width slot
        // this makes variable length column size easy to calculate
        return slice.getShort(getOffset(types.size()));
    }

    public long getLong(Slice slice, int index)
    {
        checkState(types.get(index) == FIXED_INT_64, "Expected FIXED_INT_64");

        return slice.getLong(getOffset(index));
    }

    public Slice getSlice(Slice slice, int index)
    {
        checkState(types.get(index) == VARIABLE_BINARY, "Expected VARIABLE_BINARY");

        int start = slice.getShort(getOffset(index));

        // this works because positions of variable length fields are laid out in the same order as the actual data
        int end = slice.getShort(getOffset(index) + SIZE_OF_SHORT);
        return slice.slice(start, end - start);
    }

    private int getOffset(int index)
    {
        return offsets.get(index);
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
        final StringBuilder sb = new StringBuilder();
        sb.append("TupleInfo");
        sb.append("{lengths=").append(types);
        sb.append('}');
        return sb.toString();
    }
}
