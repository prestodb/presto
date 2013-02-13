/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.split;

import com.facebook.presto.hive.HiveClientConfig;
import com.google.common.collect.ImmutableMap;
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
                .setMetastoreCacheTtl(new Duration(1, TimeUnit.HOURS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.max-chunk-size", "256MB")
                .put("hive.max-outstanding-chunks", "10")
                .put("hive.max-chunk-iterator-threads", "2")
                .put("hive.metastore-cache-ttl", "2h")
                .build();

        HiveClientConfig expected = new HiveClientConfig()
                .setMaxChunkSize(new DataSize(256, Unit.MEGABYTE))
                .setMaxOutstandingChunks(10)
                .setMaxChunkIteratorThreads(2)
                .setMetastoreCacheTtl(new Duration(2, TimeUnit.HOURS));

        ConfigAssertions.assertFullMapping(properties, expected);
    }

}
