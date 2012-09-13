package com.facebook.presto.ingest;

import com.facebook.presto.block.BlockStream;

import java.io.Closeable;

public interface RowSource
        extends BlockStream, Closeable
{
}
