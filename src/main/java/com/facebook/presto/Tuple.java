package com.facebook.presto;

import com.facebook.presto.TupleInfo.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Charsets.UTF_8;

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

    /**
     * Materializes the tuple values as Java Object.
     * This method is mainly for diagnostics and should not be called in normal query processing.
     */
    public List<Object> toValues()
    {
        ImmutableList.Builder<Object> values = ImmutableList.builder();
        int index = 0;
        for (Type type : tupleInfo.getTypes()) {
            switch (type) {
                case FIXED_INT_64:
                    values.add(getLong(index));
                    break;
                case VARIABLE_BINARY:
                    values.add(getSlice(index).toString(UTF_8));
                    break;
            }
            index++;
        }
        return values.build();
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
