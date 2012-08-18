package com.facebook.presto;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.util.List;

public class TupleInfo
{
    private final int size;
    private final List<Integer> lengths;
    private final List<Integer> offsets;

    public TupleInfo(int... lengths)
    {
        this(Ints.asList(lengths));
    }

    public TupleInfo(List<Integer> lengths)
    {
        Preconditions.checkNotNull(lengths, "lengths is null");

        this.lengths = ImmutableList.copyOf(lengths);
        int rowLength = 0;
        for (int i = 0; i < lengths.size(); i++) {
            int length = lengths.get(i);
            Preconditions.checkArgument(length >= 1, "length %s must be at least 1", i);
            rowLength += length;

        }

        ImmutableList.Builder<Integer> offsets = ImmutableList.builder();
        int current = 0;
        for (int length : lengths) {
            offsets.add(current);
            current += length;
        }
        this.offsets = offsets.build();

        this.size = rowLength;
    }

    public List<Integer> getLengths()
    {
        return lengths;
    }

    public int getLength(int index)
    {
        return lengths.get(index);
    }

    public List<Integer> getOffsets()
    {
        return offsets;
    }

    public int getOffset(int index)
    {
        return offsets.get(index);
    }

    public int size()
    {
        return size;
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

        if (!lengths.equals(tupleInfo.lengths)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return lengths.hashCode();
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("TupleInfo");
        sb.append("{lengths=").append(lengths);
        sb.append('}');
        return sb.toString();
    }
}
