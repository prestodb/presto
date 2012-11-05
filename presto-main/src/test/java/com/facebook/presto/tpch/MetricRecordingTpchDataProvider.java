package com.facebook.presto.tpch;

import com.facebook.presto.serde.FileBlocksSerde.FileEncoding;
import com.facebook.presto.tpch.TpchSchema.Column;
import com.google.common.base.Preconditions;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class MetricRecordingTpchDataProvider
        implements TpchDataProvider
{
    private final TpchDataProvider tpchDataProvider;
    private long dataFetchElapsedMillis;
    private long cumulativeDataByteSize;

    public MetricRecordingTpchDataProvider(TpchDataProvider tpchDataProvider)
    {
        this.tpchDataProvider = Preconditions.checkNotNull(tpchDataProvider, "tpchDataProvider is null");
    }

    @Override
    public File getColumnFile(Column column, FileEncoding encoding)
    {
        Preconditions.checkNotNull(column, "column is null");
        Preconditions.checkNotNull(encoding, "encoding is null");
        long start = System.nanoTime();
        try {
            File file = tpchDataProvider.getColumnFile(column, encoding);
            cumulativeDataByteSize += file.length();
            return file;
        } finally {
            dataFetchElapsedMillis += Duration.nanosSince(start).toMillis();
        }
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
