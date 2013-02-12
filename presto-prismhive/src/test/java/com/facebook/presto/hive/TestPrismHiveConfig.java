package com.facebook.presto.hive;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestPrismHiveConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(PrismHiveConfig.class)
                .setCacheTtl(new Duration(1, TimeUnit.HOURS))
                .setPrismSmcTier("prism.nssr"));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("prism.cache-ttl", "1s")
                .put("prism.smc-tier", "newtier")
                .build();

        PrismHiveConfig expected = new PrismHiveConfig()
                .setCacheTtl(new Duration(1, TimeUnit.SECONDS))
                .setPrismSmcTier("newtier");

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
