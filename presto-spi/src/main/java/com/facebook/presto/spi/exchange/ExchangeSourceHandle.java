package com.facebook.presto.spi.exchange;

public interface ExchangeSourceHandle
{
    int getPartitionId();

    long getDataSizeInBytes();

    long getRetainedSizeInBytes();
}
