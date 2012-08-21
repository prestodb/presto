package com.facebook.presto;

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

    public long getLong(int index)
    {
        return tupleInfo.getLong(slice, index);
    }

    public Slice getSlice(int index)
    {
        return tupleInfo.getSlice(slice, index);
    }

    public int size()
    {
        return tupleInfo.size(slice);
    }

    public void writeTo(SliceOutput out)
    {
        out.writeBytes(slice);
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
