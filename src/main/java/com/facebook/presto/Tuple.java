package com.facebook.presto;

import com.google.common.base.Preconditions;

public class Tuple
{
    private final Slice slice;
    private final TupleInfo tupleInfo;

    public Tuple(Slice slice, TupleInfo tupleInfo)
    {
        this.slice = slice;
        this.tupleInfo = tupleInfo;
    }

    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    public byte getByteValue(int index)
    {
        checkIndexSize(index, SizeOf.SIZE_OF_BYTE);
        return slice.getByte(tupleInfo.getOffset(index));
    }

    public int getInt(int index)
    {
        checkIndexSize(index, SizeOf.SIZE_OF_BYTE);

        return slice.getInt(tupleInfo.getOffset(index));
    }

    public long getLong(int index)
    {
        checkIndexSize(index, SizeOf.SIZE_OF_LONG);
        return slice.getLong(tupleInfo.getOffset(index));
    }

    public Slice getSlice(int index)
    {
        Preconditions.checkArgument(index < tupleInfo.size());
        return slice.slice(tupleInfo.getOffset(index), tupleInfo.getLength(index));
    }

    public void writeTo(SliceOutput out)
    {
        out.writeBytes(slice);
    }

    private void checkIndexSize(int index, int size)
    {
        Preconditions.checkArgument(index < tupleInfo.size());
        Preconditions.checkArgument(tupleInfo.getLength(index) == size, "Value %s must be %s bytes wide, but is %s bytes", index, size, tupleInfo.getLength(index));
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

        Tuple tuple = (Tuple) o;

        if (!tupleInfo.equals(tuple.tupleInfo)) {
            return false;
        }
        if (!slice.equals(tuple.slice)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = slice.hashCode();
        result = 31 * result + tupleInfo.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("Tuple");
        sb.append("{tupleInfo=").append(tupleInfo);
        sb.append(", slice=").append(slice);
        sb.append('}');
        return sb.toString();
    }
}
