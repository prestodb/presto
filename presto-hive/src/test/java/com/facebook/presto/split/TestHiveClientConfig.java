/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.split;

import com.facebook.presto.hive.HiveClientConfig;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestHiveClientConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(HiveClientConfig.class)
                .setMaxChunkSize(new DataSize(1, Unit.GIGABYTE))
                .setMaxOutstandingChunks(10_000)
                .setMaxChunkIteratorThreads(50)
                .setMetastoreCacheTtl(new Duration(1, TimeUnit.HOURS))
                .setMetastoreSocksProxy(null)
                .setMetastoreTimeout(new Duration(10, TimeUnit.SECONDS))
                .setPartitionBatchSize(500)
                .setDfsTimeout(new Duration(10, TimeUnit.SECONDS))
                .setFileSystemCacheTtl(new Duration(1, TimeUnit.DAYS))
                .setSlowDatanodeSwitchingEnabled(true)
                .setUnfavoredNodeCacheTime(new Duration(1, TimeUnit.MINUTES))
                .setGlobalDistributionDecay(new Duration(5, TimeUnit.MINUTES))
                .setStreamRateDecay(new Duration(1, TimeUnit.MINUTES))
                .setMinMonitorThreshold(new DataSize(8, Unit.KILOBYTE))
                .setMinStreamSamplingTime(new Duration(4, TimeUnit.SECONDS))
                .setMinGlobalSamples(100)
                .setMinStreamRate(new DataSize(10, Unit.KILOBYTE))
                .setSlowStreamPercentile(5));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.max-chunk-size", "256MB")
                .put("hive.max-outstanding-chunks", "10")
                .put("hive.max-chunk-iterator-threads", "2")
                .put("hive.metastore-cache-ttl", "2h")
                .put("hive.metastore.thrift.client.socks-proxy", "localhost:1080")
                .put("hive.metastore-timeout", "20s")
                .put("hive.metastore.partition-batch-size", "1000")
                .put("hive.dfs-timeout", "33s")
                .put("hive.file-system-cache-ttl", "2d")
                .put("hive.slow-datanode-switcher.enabled", "false")
                .put("hive.slow-datanode-switcher.unfavored-node-cache-time", "10m")
                .put("hive.slow-datanode-switcher.global-distribution-decay", "1h")
                .put("hive.slow-datanode-switcher.stream-rate-decay", "1s")
                .put("hive.slow-datanode-switcher.min-monitor-threshold", "1kB")
                .put("hive.slow-datanode-switcher.min-stream-sampling-time", "1h")
                .put("hive.slow-datanode-switcher.min-global-samples", "10")
                .put("hive.slow-datanode-switcher.min-stream-rate", "10B")
                .put("hive.slow-datanode-switcher.slow-stream-percentile", "25")
                .build();

        HiveClientConfig expected = new HiveClientConfig()
                .setMaxChunkSize(new DataSize(256, Unit.MEGABYTE))
                .setMaxOutstandingChunks(10)
                .setMaxChunkIteratorThreads(2)
                .setMetastoreCacheTtl(new Duration(2, TimeUnit.HOURS))
                .setMetastoreSocksProxy(HostAndPort.fromParts("localhost", 1080))
                .setMetastoreTimeout(new Duration(20, TimeUnit.SECONDS))
                .setPartitionBatchSize(1000)
                .setDfsTimeout(new Duration(33, TimeUnit.SECONDS))
                .setFileSystemCacheTtl(new Duration(2, TimeUnit.DAYS))
                .setSlowDatanodeSwitchingEnabled(false)
                .setUnfavoredNodeCacheTime(new Duration(10, TimeUnit.MINUTES))
                .setGlobalDistributionDecay(new Duration(1, TimeUnit.HOURS))
                .setStreamRateDecay(new Duration(1, TimeUnit.SECONDS))
                .setMinMonitorThreshold(new DataSize(1, Unit.KILOBYTE))
                .setMinStreamSamplingTime(new Duration(1, TimeUnit.HOURS))
                .setMinGlobalSamples(10)
                .setMinStreamRate(new DataSize(10, Unit.BYTE))
                .setSlowStreamPercentile(25);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
