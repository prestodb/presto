package com.facebook.presto.execution;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestQueryManagerConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(QueryManagerConfig.class)
                .setCoordinator(true)
                .setMaxShardProcessorThreads(Runtime.getRuntime().availableProcessors() * 4)
                .setMaxQueryAge(new Duration(15, TimeUnit.MINUTES))
                .setClientTimeout(new Duration(5, TimeUnit.MINUTES))
                .setMaxOperatorMemoryUsage(new DataSize(256, Unit.MEGABYTE))
                .setMaxPendingSplitsPerNode(100)
                .setExchangeMaxBufferSize(new DataSize(32, Unit.MEGABYTE))
                .setExchangeConcurrentRequestMultiplier(3)
                .setQueryManagerExecutorPoolSize(100)
                .setSinkMaxBufferedPages(10)
                .setRemoteTaskMaxConsecutiveErrorCount(10)
                .setRemoteTaskMinErrorDuration(new Duration(2, TimeUnit.MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("coordinator", "false")
                .put("query.operator.max-memory", "1GB")
                .put("query.shard.max-threads", "3")
                .put("query.client.timeout", "10s")
                .put("query.max-age", "30s")
                .put("query.max-pending-splits-per-node", "33")
                .put("query.manager-executor-pool-size", "11")
                .put("sink.page-buffer-max", "999")
                .put("exchange.page-buffer-size", "1GB")
                .put("exchange.concurrent-request-multiplier", "13")
                .put("query.remote-task.max-consecutive-error-count", "300")
                .put("query.remote-task.min-error-duration", "30s")
                .build();

        QueryManagerConfig expected = new QueryManagerConfig()
                .setCoordinator(false)
                .setMaxOperatorMemoryUsage(new DataSize(1, Unit.GIGABYTE))
                .setMaxShardProcessorThreads(3)
                .setMaxQueryAge(new Duration(30, TimeUnit.SECONDS))
                .setClientTimeout(new Duration(10, TimeUnit.SECONDS))
                .setMaxPendingSplitsPerNode(33)
                .setExchangeMaxBufferSize(new DataSize(1, Unit.GIGABYTE))
                .setExchangeConcurrentRequestMultiplier(13)
                .setQueryManagerExecutorPoolSize(11)
                .setSinkMaxBufferedPages(999)
                .setRemoteTaskMaxConsecutiveErrorCount(300)
                .setRemoteTaskMinErrorDuration(new Duration(30, TimeUnit.SECONDS));

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
