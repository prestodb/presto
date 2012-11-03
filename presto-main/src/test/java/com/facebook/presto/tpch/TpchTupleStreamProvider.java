package com.facebook.presto.tpch;

import com.facebook.presto.nblock.BlockIterable;
import com.facebook.presto.serde.BlockSerdes;

public interface TpchTupleStreamProvider
{
    BlockIterable getBlocks(TpchSchema.Column column, BlockSerdes.Encoding encoding);
}
