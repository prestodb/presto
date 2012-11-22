package com.facebook.presto.tpch;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.serde.BlocksFileEncoding;
import io.airlift.units.DataSize;

public interface TpchBlocksProvider
{
    BlockIterable getBlocks(TpchTableHandle tableHandle, TpchColumnHandle columnHandle, BlocksFileEncoding encoding);

    DataSize getColumnDataSize(TpchTableHandle tableHandle, TpchColumnHandle columnHandle, BlocksFileEncoding encoding);
}
