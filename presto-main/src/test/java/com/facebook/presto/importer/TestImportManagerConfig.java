package com.facebook.presto.importer;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.util.Map;

public class TestImportManagerConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(ImportManagerConfig.class)
                .setMaxPartitionThreads(50)
                .setMaxChunkThreads(50)
                .setMaxShardThreads(50));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("import.max-partition-threads", "51")
                .put("import.max-chunk-threads", "52")
                .put("import.max-shard-threads", "53")
                .build();

        ImportManagerConfig expected = new ImportManagerConfig()
                .setMaxPartitionThreads(51)
                .setMaxChunkThreads(52)
                .setMaxShardThreads(53);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
