package com.facebook.presto;

import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceOutput;

public interface BlockStreamSerde<T extends ValueBlock>
{
    void serialize(BlockStream<? extends ValueBlock> blockStream, SliceOutput sliceOutput);
    BlockStream<T> deserialize(Slice slice);
}
