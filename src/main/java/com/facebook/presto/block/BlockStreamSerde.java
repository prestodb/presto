package com.facebook.presto.block;

import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceOutput;

public interface BlockStreamSerde<T extends Block>
{
    void serialize(BlockStream<? extends Block> blockStream, SliceOutput sliceOutput);
    BlockStream<T> deserialize(Slice slice);
}
