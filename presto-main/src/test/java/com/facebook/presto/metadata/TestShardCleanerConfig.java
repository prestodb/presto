package com.facebook.presto.metadata;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestShardCleanerConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(ShardCleanerConfig.class)
                .setEnabled(false)
                .setCleanerInterval(new Duration(60, TimeUnit.SECONDS))
                .setMaxThreads(32));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("shard-cleaner.enabled", "true")
                .put("shard-cleaner.interval", "10m")
                .put("shard-cleaner.max-threads", "100")
                .build();

        ShardCleanerConfig expected = new ShardCleanerConfig()
                .setEnabled(true)
                .setCleanerInterval(new Duration(10, TimeUnit.MINUTES))
                .setMaxThreads(100);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
