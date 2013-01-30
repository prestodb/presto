/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.split;

import com.facebook.presto.hive.HiveClientConfig;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.testng.annotations.Test;

import java.util.Map;

public class TestHiveClientConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(HiveClientConfig.class)
                .setMaxChunkSize(new DataSize(1, Unit.GIGABYTE))
                .setMaxOutstandingChunks(10_000)
                .setMaxChunkIteratorThreads(50));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.max-chunk-size", "256MB")
                .put("hive.max-outstanding-chunks", "10")
                .put("hive.max-chunk-iterator-threads", "2")
                .build();

        HiveClientConfig expected = new HiveClientConfig()
                .setMaxChunkSize(new DataSize(256, Unit.MEGABYTE))
                .setMaxOutstandingChunks(10)
                .setMaxChunkIteratorThreads(2);

        ConfigAssertions.assertFullMapping(properties, expected);
    }

}
