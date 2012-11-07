package com.facebook.presto.tpch;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.tpch.TpchSchema.Column;
import io.airlift.units.DataSize;

public interface TpchBlocksProvider
{
    BlockIterable getBlocks(TpchSchema.Column column, BlocksFileEncoding encoding);

    DataSize getColumnDataSize(Column column, BlocksFileEncoding encoding);
}
