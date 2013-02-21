package com.facebook.presto.hive;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestPrismConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(PrismConfig.class)
                .setCacheTtl(new Duration(1, TimeUnit.HOURS))
                .setPrismSmcTier("prism.nssr")
                .setAllowedRegions(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("prism.cache-ttl", "1s")
                .put("prism.smc-tier", "newtier")
                .put("prism.allowed-regions", "allowedregion1, allowedregion2")
                .build();

        PrismConfig expected = new PrismConfig()
                .setCacheTtl(new Duration(1, TimeUnit.SECONDS))
                .setPrismSmcTier("newtier")
                .setAllowedRegions("allowedregion1, allowedregion2");

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
