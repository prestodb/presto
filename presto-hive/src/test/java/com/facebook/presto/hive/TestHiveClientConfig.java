/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.hive;

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
                .setMaxSplitSize(new DataSize(1, Unit.GIGABYTE))
                .setMaxOutstandingSplits(10_000)
                .setMaxSplitIteratorThreads(50)
                .setMetastoreCacheTtl(new Duration(1, TimeUnit.HOURS))
                .setMetastoreRefreshInterval(new Duration(2, TimeUnit.MINUTES))
                .setMaxMetastoreRefreshThreads(100)
                .setMetastoreSocksProxy(null)
                .setMetastoreTimeout(new Duration(10, TimeUnit.SECONDS))
                .setMinPartitionBatchSize(10)
                .setMaxPartitionBatchSize(100)
                .setDfsTimeout(new Duration(10, TimeUnit.SECONDS))
                .setDfsConnectTimeout(new Duration(500, TimeUnit.MILLISECONDS))
                .setDfsConnectMaxRetries(5)
                .setFileSystemCacheTtl(new Duration(1, TimeUnit.DAYS))
                .setDomainSocketPath(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.max-split-size", "256MB")
                .put("hive.max-outstanding-splits", "10")
                .put("hive.max-split-iterator-threads", "2")
                .put("hive.metastore-cache-ttl", "2h")
                .put("hive.metastore-refresh-interval", "30m")
                .put("hive.metastore-refresh-max-threads", "2500")
                .put("hive.metastore.thrift.client.socks-proxy", "localhost:1080")
                .put("hive.metastore-timeout", "20s")
                .put("hive.metastore.partition-batch-size.min", "1")
                .put("hive.metastore.partition-batch-size.max", "1000")
                .put("hive.dfs-timeout", "33s")
                .put("hive.dfs.connect.timeout", "20s")
                .put("hive.dfs.connect.max-retries", "10")
                .put("hive.file-system-cache-ttl", "2d")
                .put("dfs.domain-socket-path", "/foo")
                .build();

        HiveClientConfig expected = new HiveClientConfig()
                .setMaxSplitSize(new DataSize(256, Unit.MEGABYTE))
                .setMaxOutstandingSplits(10)
                .setMaxSplitIteratorThreads(2)
                .setMetastoreCacheTtl(new Duration(2, TimeUnit.HOURS))
                .setMetastoreRefreshInterval(new Duration(30, TimeUnit.MINUTES))
                .setMaxMetastoreRefreshThreads(2500)
                .setMetastoreSocksProxy(HostAndPort.fromParts("localhost", 1080))
                .setMetastoreTimeout(new Duration(20, TimeUnit.SECONDS))
                .setMinPartitionBatchSize(1)
                .setMaxPartitionBatchSize(1000)
                .setDfsTimeout(new Duration(33, TimeUnit.SECONDS))
                .setDfsConnectTimeout(new Duration(20, TimeUnit.SECONDS))
                .setDfsConnectMaxRetries(10)
                .setFileSystemCacheTtl(new Duration(2, TimeUnit.DAYS))
                .setDomainSocketPath("/foo");

        ConfigAssertions.assertFullMapping(properties, expected);
    }

    @Test
    public void testDeprecatedProperties()
    {
        Map<String, String> currentProperties = new ImmutableMap.Builder<String, String>()
                .put("hive.max-split-size", "256MB")
                .put("hive.max-outstanding-splits", "10")
                .put("hive.max-split-iterator-threads", "2")
                .build();

        Map<String, String> oldProperties = new ImmutableMap.Builder<String, String>()
                .put("hive.max-chunk-size", "256MB")
                .put("hive.max-outstanding-chunks", "10")
                .put("hive.max-chunk-iterator-threads", "2")
                .build();

        ConfigAssertions.assertDeprecatedEquivalence(HiveClientConfig.class, currentProperties, oldProperties);
    }
}
