package com.facebook.presto.event.scribe.client;

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class ScribeClientConfiguration
{
    private int maxQueueLength = 1_000_000;
    private DataSize maxBatchSize = DataSize.valueOf("10MB");

    @Min(1)
    public int getMaxQueueLength()
    {
        return this.maxQueueLength;
    }

    @Config("scribe.max-queue-length")
    public ScribeClientConfiguration setMaxQueueLength(int maxQueueLength)
    {
        this.maxQueueLength = maxQueueLength;
        return this;
    }

    @NotNull
    public DataSize getMaxBatchSize()
    {
        return maxBatchSize;
    }

    @Config("scribe.max-batch-size")
    public ScribeClientConfiguration setMaxBatchSize(DataSize maxBatchSize)
    {
        this.maxBatchSize = maxBatchSize;
        return this;
    }
}
