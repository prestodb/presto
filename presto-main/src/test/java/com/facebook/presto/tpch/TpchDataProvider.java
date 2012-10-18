package com.facebook.presto.tpch;

import com.facebook.presto.block.TupleStreamSerializer;

import java.io.File;

public interface TpchDataProvider
{
    File getColumnFile(TpchSchema.Column column, TupleStreamSerializer serializer, String serdeName);
}
