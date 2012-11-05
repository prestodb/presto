package com.facebook.presto.tpch;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.serde.FileBlocksSerde.FileEncoding;

public interface TpchBlocksProvider
{
    BlockIterable getBlocks(TpchSchema.Column column, FileEncoding encoding);
}
