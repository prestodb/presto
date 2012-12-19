package com.facebook.presto.execution;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.testng.annotations.Test;

import java.util.Map;

public class TestQueryManagerConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(QueryManagerConfig.class)
                .setImportsEnabled(true)
                .setMaxOperatorMemoryUsage(new DataSize(256, Unit.MEGABYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("import.enabled", "false")
                .put("query.operator.max-memory", "1GB")
                .build();

        QueryManagerConfig expected = new QueryManagerConfig()
                .setMaxOperatorMemoryUsage(new DataSize(1, Unit.GIGABYTE))
                .setImportsEnabled(false);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
