package com.facebook.presto.raptor;

import com.facebook.presto.raptor.storage.RowSink;
import com.facebook.presto.raptor.storage.RowSinkProvider;

public class CappedRowSink
        implements RowSink
{
    private final RowSinkProvider rowSinkProvider;
    private final int rowsPerShard;

    private int currentRowCount;
    private RowSink delegate;

    private CappedRowSink(RowSinkProvider rowSinkProvider, int rowsPerShard)
    {
        this.rowSinkProvider = rowSinkProvider;
        this.rowsPerShard = rowsPerShard;

        this.delegate = rowSinkProvider.getRowSink();
        this.currentRowCount = 0;
    }

    public static RowSink from(RowSinkProvider rowSinkProvider, int rowsPerShard)
    {
        return new CappedRowSink(rowSinkProvider, rowsPerShard);
    }

    @Override
    public void beginRecord(long sampleWeight)
    {
        if (currentRowCount >= rowsPerShard) {
            delegate.close();
            delegate = rowSinkProvider.getRowSink();
            currentRowCount = 0;
        }
        delegate.beginRecord(sampleWeight);
    }

    @Override
    public void finishRecord()
    {
        delegate.finishRecord();
        currentRowCount++;
    }

    @Override
    public int currentField()
    {
        return delegate.currentField();
    }

    @Override
    public void appendNull()
    {
        delegate.appendNull();
    }

    @Override
    public void appendBoolean(boolean value)
    {
        delegate.appendBoolean(value);
    }

    @Override
    public void appendLong(long value)
    {
        delegate.appendLong(value);
    }

    @Override
    public void appendDouble(double value)
    {
        delegate.appendDouble(value);
    }

    @Override
    public void appendString(String value)
    {
        delegate.appendString(value);
    }

    @Override
    public void appendBytes(byte[] value)
    {
        delegate.appendBytes(value);
    }

    @Override
    public void close()
    {
        delegate.close();
    }
}
