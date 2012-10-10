package com.facebook.presto.tpch;

import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamSerdes;

public interface TpchTupleStreamProvider
{
    TupleStream getTupleStream(TpchSchema.Column column, TupleStreamSerdes.Encoding encoding);
}
