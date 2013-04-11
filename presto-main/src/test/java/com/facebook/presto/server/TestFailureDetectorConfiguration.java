package com.facebook.presto.server;

import com.facebook.presto.failureDetector.FailureDetectorConfiguration;
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
                .setExpirationGraceInterval(new Duration(10, TimeUnit.MINUTES))
                .setFailureRatioThreshold(0.01)
                .setHeartbeatInterval(new Duration(500, TimeUnit.MILLISECONDS))
                .setWarmupInterval(new Duration(5, TimeUnit.SECONDS))
                .setEnabled(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("failure-detector.expiration-grace-interval", "5m")
                .put("failure-detector.warmup-interval", "60s")
                .put("failure-detector.heartbeat-interval", "10s")
                .put("failure-detector.threshold", "0.5")
                .put("failure-detector.enabled", "false")
                .build();

        FailureDetectorConfiguration expected = new FailureDetectorConfiguration()
                .setExpirationGraceInterval(new Duration(5, TimeUnit.MINUTES))
                .setWarmupInterval(new Duration(60, TimeUnit.SECONDS))
                .setHeartbeatInterval(new Duration(10, TimeUnit.SECONDS))
                .setFailureRatioThreshold(0.5)
                .setEnabled(false);

        ConfigAssertions.assertFullMapping(properties, expected);
    }

}
