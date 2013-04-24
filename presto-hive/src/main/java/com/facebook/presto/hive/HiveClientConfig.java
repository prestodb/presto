/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.hive;

import com.google.common.net.HostAndPort;
import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.concurrent.TimeUnit;

public class HiveClientConfig
{
    private DataSize maxChunkSize = new DataSize(1, Unit.GIGABYTE);
    private int maxOutstandingChunks = 10_000;
    private int maxChunkIteratorThreads = 50;
    private int partitionBatchSize = 500;
    private Duration metastoreCacheTtl = new Duration(1, TimeUnit.HOURS);
    private HostAndPort metastoreSocksProxy;
    private Duration metastoreTimeout = new Duration(10, TimeUnit.SECONDS);

    private Duration fileSystemCacheTtl = new Duration(1, TimeUnit.DAYS);
    private Duration dfsTimeout = new Duration(10, TimeUnit.SECONDS);

    private boolean slowDatanodeSwitchingEnabled = true;
    private Duration unfavoredNodeCacheTime = new Duration(1, TimeUnit.MINUTES);
    private Duration globalDistributionDecay = new Duration(5, TimeUnit.MINUTES);
    private Duration streamRateDecay = new Duration(1, TimeUnit.MINUTES);
    private DataSize minMonitorThreshold = new DataSize(8, Unit.KILOBYTE);
    private Duration minStreamSamplingTime = new Duration(4, TimeUnit.SECONDS);
    private int minGlobalSamples = 100;
    private DataSize minStreamRate = new DataSize(10, Unit.KILOBYTE);
    private int slowStreamPercentile = 5;

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

    @Min(1)
    public int getPartitionBatchSize()
    {
        return partitionBatchSize;
    }

    @Config("hive.metastore.partition-batch-size")
    public HiveClientConfig setPartitionBatchSize(int partitionBatchSize)
    {
        this.partitionBatchSize = partitionBatchSize;
        return this;
    }

    @NotNull
    public Duration getFileSystemCacheTtl()
    {
        return fileSystemCacheTtl;
    }

    @Config("hive.file-system-cache-ttl")
    public HiveClientConfig setFileSystemCacheTtl(Duration fileSystemCacheTtl)
    {
        this.fileSystemCacheTtl = fileSystemCacheTtl;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getDfsTimeout()
    {
        return dfsTimeout;
    }

    @Config("hive.dfs-timeout")
    public HiveClientConfig setDfsTimeout(Duration dfsTimeout)
    {
        this.dfsTimeout = dfsTimeout;
        return this;
    }

    public boolean getSlowDatanodeSwitchingEnabled()
    {
        return slowDatanodeSwitchingEnabled;
    }

    @Config("hive.slow-datanode-switcher.enabled")
    public HiveClientConfig setSlowDatanodeSwitchingEnabled(boolean slowDatanodeSwitchingEnabled)
    {
        this.slowDatanodeSwitchingEnabled = slowDatanodeSwitchingEnabled;
        return this;
    }

    @NotNull
    public Duration getUnfavoredNodeCacheTime()
    {
        return unfavoredNodeCacheTime;
    }

    @Config("hive.slow-datanode-switcher.unfavored-node-cache-time")
    public HiveClientConfig setUnfavoredNodeCacheTime(Duration unfavoredNodeCacheTime)
    {
        this.unfavoredNodeCacheTime = unfavoredNodeCacheTime;
        return this;
    }

    @NotNull
    @MinDuration("1s")
    public Duration getGlobalDistributionDecay()
    {
        return globalDistributionDecay;
    }

    @Config("hive.slow-datanode-switcher.global-distribution-decay")
    public HiveClientConfig setGlobalDistributionDecay(Duration globalDistributionDecay)
    {
        this.globalDistributionDecay = globalDistributionDecay;
        return this;
    }

    @NotNull
    @MinDuration("1s")
    public Duration getStreamRateDecay()
    {
        return streamRateDecay;
    }

    @Config("hive.slow-datanode-switcher.stream-rate-decay")
    public HiveClientConfig setStreamRateDecay(Duration streamRateDecay)
    {
        this.streamRateDecay = streamRateDecay;
        return this;
    }

    @NotNull
    public DataSize getMinMonitorThreshold()
    {
        return minMonitorThreshold;
    }

    @Config("hive.slow-datanode-switcher.min-monitor-threshold")
    public HiveClientConfig setMinMonitorThreshold(DataSize minMonitorThreshold)
    {
        this.minMonitorThreshold = minMonitorThreshold;
        return this;
    }

    @NotNull
    public Duration getMinStreamSamplingTime()
    {
        return minStreamSamplingTime;
    }

    @Config("hive.slow-datanode-switcher.min-stream-sampling-time")
    public HiveClientConfig setMinStreamSamplingTime(Duration minStreamSamplingTime)
    {
        this.minStreamSamplingTime = minStreamSamplingTime;
        return this;
    }

    @Min(1)
    public int getMinGlobalSamples()
    {
        return minGlobalSamples;
    }

    @Config("hive.slow-datanode-switcher.min-global-samples")
    public HiveClientConfig setMinGlobalSamples(int minGlobalSamples)
    {
        this.minGlobalSamples = minGlobalSamples;
        return this;
    }

    @NotNull
    public DataSize getMinStreamRate()
    {
        return minStreamRate;
    }

    @Config("hive.slow-datanode-switcher.min-stream-rate")
    public HiveClientConfig setMinStreamRate(DataSize minStreamRate)
    {
        this.minStreamRate = minStreamRate;
        return this;
    }

    @Min(1)
    @Max(99)
    public int getSlowStreamPercentile()
    {
        return slowStreamPercentile;
    }

    @Config("hive.slow-datanode-switcher.slow-stream-percentile")
    public HiveClientConfig setSlowStreamPercentile(int slowStreamPercentile)
    {
        this.slowStreamPercentile = slowStreamPercentile;
        return this;
    }
}
