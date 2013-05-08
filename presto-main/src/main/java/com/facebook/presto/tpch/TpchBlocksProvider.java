package com.facebook.presto.tpch;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.serde.BlocksFileEncoding;

public interface TpchBlocksProvider
{
    BlockIterable getBlocks(TpchTableHandle tableHandle,
            TpchColumnHandle columnHandle,
            int partNumber,
            int totalParts,
            BlocksFileEncoding encoding);
}
