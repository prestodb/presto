package com.facebook.presto.tpch;

import com.google.common.annotations.VisibleForTesting;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.serde.BlocksFileEncoding;
import io.airlift.units.DataSize;

public abstract class TpchBlocksProvider
{
    public abstract BlockIterable getBlocks(TpchTableHandle tableHandle,
            TpchColumnHandle columnHandle,
            int partNumber,
            int totalParts,
            BlocksFileEncoding encoding);

    @VisibleForTesting
    public final BlockIterable getBlocks(TpchTableHandle tableHandle,
            TpchColumnHandle columnHandle,
            BlocksFileEncoding encoding)
    {
        return getBlocks(tableHandle, columnHandle, 0, 1, encoding);
    }

    public abstract DataSize getColumnDataSize(TpchTableHandle tableHandle,
            TpchColumnHandle columnHandle,
            int partNumber,
            int totalParts,
            BlocksFileEncoding encoding);
}
