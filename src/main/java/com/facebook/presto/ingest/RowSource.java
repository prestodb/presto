package com.facebook.presto.ingest;

import com.facebook.presto.block.BlockStream;
import com.facebook.presto.block.Block;

import java.io.Closeable;

public interface RowSource
        extends BlockStream<Block>, Closeable
{
}
