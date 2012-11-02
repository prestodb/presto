package com.facebook.presto.tpch;

import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamSerdes;
import com.facebook.presto.nblock.BlockIterable;
import com.facebook.presto.serde.BlockSerdes;

public interface TpchTupleStreamProvider
{
    TupleStream getTupleStream(TpchSchema.Column column, TupleStreamSerdes.Encoding encoding);
    BlockIterable getBlocks(TpchSchema.Column column, BlockSerdes.Encoding encoding);
}
