package com.facebook.presto.tpch;

import com.facebook.presto.serde.BlocksFileEncoding;

import java.io.File;

public interface TpchDataProvider
{
    File getColumnFile(TpchSchema.Column column, BlocksFileEncoding encoding);
}
