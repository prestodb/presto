package com.facebook.presto.tpch;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.tpch.TpchSchema.Column;
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
    public BlockIterable getBlocks(Column column, BlocksFileEncoding encoding)
    {
        Preconditions.checkNotNull(column, "column is null");
        Preconditions.checkNotNull(encoding, "encoding is null");
        long start = System.nanoTime();
        try {
            BlockIterable blocks = tpchBlocksProvider.getBlocks(column, encoding);
            cumulativeDataByteSize += tpchBlocksProvider.getColumnDataSize(column, encoding).toBytes();
            return blocks;
        } finally {
            dataFetchElapsedMillis += Duration.nanosSince(start).toMillis();
        }
    }

    @Override
    public DataSize getColumnDataSize(Column column, BlocksFileEncoding encoding)
    {
        return tpchBlocksProvider.getColumnDataSize(column, encoding);
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
