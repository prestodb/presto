package com.facebook.presto.tpch;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.util.CpuTimer;
import com.facebook.presto.util.CpuTimer.CpuDuration;
import com.google.common.base.Preconditions;
import io.airlift.units.DataSize;

public class MetricRecordingTpchBlocksProvider
        implements TpchBlocksProvider
{
    private final TpchBlocksProvider tpchBlocksProvider;
    private CpuDuration dataFetchCpuDuration = new CpuDuration();
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
        CpuTimer cpuTimer = new CpuTimer();
        try {
            BlockIterable blocks = tpchBlocksProvider.getBlocks(tableHandle, columnHandle, encoding);
            cumulativeDataByteSize += tpchBlocksProvider.getColumnDataSize(tableHandle, columnHandle, encoding).toBytes();
            return blocks;
        } finally {
            dataFetchCpuDuration = dataFetchCpuDuration.add(cpuTimer.elapsedTime());
        }
    }

    @Override
    public DataSize getColumnDataSize(TpchTableHandle tableHandle, TpchColumnHandle columnHandle, BlocksFileEncoding encoding)
    {
        return tpchBlocksProvider.getColumnDataSize(tableHandle, columnHandle, encoding);
    }

    public CpuDuration getDataFetchCpuDuration()
    {
        return dataFetchCpuDuration;
    }

    public DataSize getCumulativeDataSize()
    {
        return new DataSize(cumulativeDataByteSize, DataSize.Unit.BYTE);
    }
}
