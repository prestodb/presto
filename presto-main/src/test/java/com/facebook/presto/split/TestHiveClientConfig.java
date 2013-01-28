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
                .setMaxChunkSize(new DataSize(1, Unit.GIGABYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.max-chunk-size", "256MB")
                .build();

        HiveClientConfig expected = new HiveClientConfig()
                .setMaxChunkSize(new DataSize(256, Unit.MEGABYTE));

        ConfigAssertions.assertFullMapping(properties, expected);
    }

}
