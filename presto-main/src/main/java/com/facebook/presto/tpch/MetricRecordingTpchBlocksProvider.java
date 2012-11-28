package com.facebook.presto.tpch;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.google.common.base.Preconditions;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

public class MetricRecordingTpchBlocksProvider
        implements TpchBlocksProvider
{
    private final TpchBlocksProvider tpchBlocksProvider;
    private long dataFetchElapsedMillis;
    private long cumulativeDataByteSize;

    public MetricRecordingTpchBlocksProvider(TpchBlocksProvider tpchBlocksProvider)
    {
        this.tpchBlocksProvider = Preconditions.checkNotNull(tpchBlocksProvider, "tpchBlocksProvider is null");
    }

    @Override
    public BlockIterable getBlocks(TpchTableHandle tableHandle, TpchColumnHandle columnHandle, BlocksFileEncoding encoding)
    {
        Preconditions.checkNotNull(tableHandle, "tableHandle is null");
        Preconditions.checkNotNull(columnHandle, "columnHandle is null");
        Preconditions.checkNotNull(encoding, "encoding is null");
        long start = System.nanoTime();
        try {
            BlockIterable blocks = tpchBlocksProvider.getBlocks(tableHandle, columnHandle, encoding);
            cumulativeDataByteSize += tpchBlocksProvider.getColumnDataSize(tableHandle, columnHandle, encoding).toBytes();
            return blocks;
        } finally {
            dataFetchElapsedMillis += Duration.nanosSince(start).toMillis();
        }
    }

    @Override
    public DataSize getColumnDataSize(TpchTableHandle tableHandle, TpchColumnHandle columnHandle, BlocksFileEncoding encoding)
    {
        return tpchBlocksProvider.getColumnDataSize(tableHandle, columnHandle, encoding);
    }

    public Duration getDataFetchElapsedTime()
    {
        return new Duration(dataFetchElapsedMillis, TimeUnit.MILLISECONDS);
    }
    
    public DataSize getCumulativeDataSize()
    {
        return new DataSize(cumulativeDataByteSize, DataSize.Unit.BYTE);
    }
}
