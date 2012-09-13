package com.facebook.presto.block;

import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceOutput;

public interface BlockStreamSerde
{
    void serialize(BlockStream blockStream, SliceOutput sliceOutput);
    BlockStream deserialize(Slice slice);
}
