package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.tuple.TupleReadable;

public class TupleInputResolver
        implements InputResolver
{
    private TupleReadable[] inputs;

    public void setInputs(TupleReadable[] inputs)
    {
        this.inputs = inputs;
    }

    @Override
    public Object getValue(Input input)
    {
        TupleReadable tuple = inputs[input.getChannel()];
        int field = input.getField();

        if (tuple.isNull(field)) {
            return null;
        }

        switch (tuple.getTupleInfo().getTypes().get(field)) {
            case FIXED_INT_64:
                return tuple.getLong(field);
            case DOUBLE:
                return tuple.getDouble(field);
            case VARIABLE_BINARY:
                return tuple.getSlice(field);
            default:
                throw new UnsupportedOperationException("not yet implemented");
        }
    }
}
