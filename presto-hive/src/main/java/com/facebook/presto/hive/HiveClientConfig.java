/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.hive;

import com.google.common.net.HostAndPort;
import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.concurrent.TimeUnit;

public class HiveClientConfig
{
    private DataSize maxChunkSize = new DataSize(1, Unit.GIGABYTE);
    private int maxOutstandingChunks = 10_000;
    private int maxChunkIteratorThreads = 50;
    private Duration metastoreCacheTtl = new Duration(1, TimeUnit.HOURS);
    private HostAndPort metastoreSocksProxy;
    private Duration metastoreTimeout = new Duration(10, TimeUnit.SECONDS);

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

    @Min(1)
    public int getMaxOutstandingChunks()
    {
        return maxOutstandingChunks;
    }

    @Config("hive.max-outstanding-chunks")
    public HiveClientConfig setMaxOutstandingChunks(int maxOutstandingChunks)
    {
        this.maxOutstandingChunks = maxOutstandingChunks;
        return this;
    }

    @Min(1)
    public int getMaxChunkIteratorThreads()
    {
        return maxChunkIteratorThreads;
    }

    @Config("hive.max-chunk-iterator-threads")
    public HiveClientConfig setMaxChunkIteratorThreads(int maxChunkIteratorThreads)
    {
        this.maxChunkIteratorThreads = maxChunkIteratorThreads;
        return this;
    }

    @NotNull
    public Duration getMetastoreCacheTtl()
    {
        return metastoreCacheTtl;
    }

    @Config("hive.metastore-cache-ttl")
    public HiveClientConfig setMetastoreCacheTtl(Duration metastoreCacheTtl)
    {
        this.metastoreCacheTtl = metastoreCacheTtl;
        return this;
    }

    public HostAndPort getMetastoreSocksProxy()
    {
        return metastoreSocksProxy;
    }

    @Config("hive.metastore.thrift.client.socks-proxy")
    public HiveClientConfig setMetastoreSocksProxy(HostAndPort metastoreSocksProxy)
    {
        this.metastoreSocksProxy = metastoreSocksProxy;
        return this;
    }

    @NotNull
    public Duration getMetastoreTimeout()
    {
        return metastoreTimeout;
    }

    @Config("hive.metastore-timeout")
    public HiveClientConfig setMetastoreTimeout(Duration metastoreTimeout)
    {
        this.metastoreTimeout = metastoreTimeout;
        return this;
    }
}
