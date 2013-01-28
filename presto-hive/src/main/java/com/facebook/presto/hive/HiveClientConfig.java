/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.hive;

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import javax.validation.constraints.NotNull;

public class HiveClientConfig
{
    private DataSize maxChunkSize = new DataSize(1, Unit.GIGABYTE);

    @NotNull
    public DataSize getMaxChunkSize()
    {
        return maxChunkSize;
    }

    @Config("hive.max-chunk-size")
    public HiveClientConfig setMaxChunkSize(DataSize maxChunkSize)
    {
        this.maxChunkSize = maxChunkSize;
        return this;
    }
}
