package com.facebook.presto.tpch;

import com.facebook.presto.serde.FileBlocksSerde.FileEncoding;

import java.io.File;

public interface TpchDataProvider
{
    File getColumnFile(TpchSchema.Column column, FileEncoding encoding);
}
