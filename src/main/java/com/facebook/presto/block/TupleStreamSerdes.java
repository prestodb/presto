package com.facebook.presto.block;

import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceOutput;

import static com.google.common.base.Preconditions.checkNotNull;

public class TupleStreamSerdes
{
    public static void serialize(TupleStreamSerde serde, TupleStream tupleStream, SliceOutput sliceOutput)
    {
        checkNotNull(serde, "serde is null");
        checkNotNull(tupleStream, "tupleStream is null");
        checkNotNull(sliceOutput, "sliceOutput is null");
        serde.createSerializer().createTupleStreamWriter(sliceOutput).append(tupleStream).finish();
    }
    
    public static TupleStream deserialize(TupleStreamSerde serde, Slice slice)
    {
        checkNotNull(serde, "serde is null");
        return serde.createDeserializer().deserialize(slice);
    }
}
