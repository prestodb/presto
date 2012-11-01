package com.facebook.presto.tpch;

import com.facebook.presto.block.TupleStreamSerializer;
import com.facebook.presto.serde.BlockSerde;

import java.io.File;

public interface TpchDataProvider
{
    File getColumnFile(TpchSchema.Column column, TupleStreamSerializer serializer, String serdeName);
    File getColumnFile(TpchSchema.Column column, BlockSerde blockSerde, String serdeName);
}
