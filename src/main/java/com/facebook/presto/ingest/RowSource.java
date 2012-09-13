package com.facebook.presto.ingest;

import com.facebook.presto.block.BlockStream;
import com.facebook.presto.block.ValueBlock;

import java.io.Closeable;

public interface RowSource
        extends BlockStream<ValueBlock>, Closeable
{
}
