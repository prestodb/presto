package com.facebook.presto.tpch;

import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamSerdes;
import com.facebook.presto.nblock.Blocks;

public interface TpchTupleStreamProvider
{
    TupleStream getTupleStream(TpchSchema.Column column, TupleStreamSerdes.Encoding encoding);
    Blocks getBlocks(TpchSchema.Column column, TupleStreamSerdes.Encoding encoding);
}
