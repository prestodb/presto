package com.facebook.presto.block;

import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceOutput;

public interface TupleStreamSerde
{
    void serialize(TupleStream tupleStream, SliceOutput sliceOutput);
    TupleStream deserialize(Slice slice);
}
