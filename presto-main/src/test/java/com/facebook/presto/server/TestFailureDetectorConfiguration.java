package com.facebook.presto.server;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestFailureDetectorConfiguration
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(FailureDetectorConfiguration.class)
                .setFailureRatioThreshold(0.01)
                .setHearbeatInterval(new Duration(500, TimeUnit.MILLISECONDS))
                .setWarmupInterval(new Duration(5, TimeUnit.SECONDS))
                .setEnabled(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("failure-detector.warmup-interval", "60s")
                .put("failure-detector.heartbeat-interval", "10s")
                .put("failure-detector.threshold", "0.5")
                .put("failure-detector.enabled", "false")
                .build();

        FailureDetectorConfiguration expected = new FailureDetectorConfiguration()
                .setWarmupInterval(new Duration(60, TimeUnit.SECONDS))
                .setHearbeatInterval(new Duration(10, TimeUnit.SECONDS))
                .setFailureRatioThreshold(0.5)
                .setEnabled(false);

        ConfigAssertions.assertFullMapping(properties, expected);
    }

}
