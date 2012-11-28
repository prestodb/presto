package com.facebook.presto.tpch;

import com.facebook.presto.serde.BlocksFileEncoding;

import java.io.File;

public interface TpchDataProvider
{
    File getDataFile(TpchTableHandle tableHandle, TpchColumnHandle columnHandle, BlocksFileEncoding encoding);
}
