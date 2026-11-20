/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.delta;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.facebook.airlift.units.Duration;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for {@link DeltaConfig}.
 */
public class TestDeltaConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(DeltaConfig.class)
                .setMaxSplitsBatchSize(200)
                .setParquetDereferencePushdownEnabled(true)
                .setCaseSensitivePartitionsEnabled(true)
                .setTableStatisticsEnabled(false)
                .setTableStatisticsCacheMaxEntries(1_000_000)
                .setTableStatisticsCacheTtl(new Duration(1, TimeUnit.HOURS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("delta.max-splits-batch-size", "400")
                .put("delta.parquet-dereference-pushdown-enabled", "false")
                .put("delta.case-sensitive-partitions-enabled", "false")
                .put("delta.table-statistics-enabled", "true")
                .put("delta.table-statistics-cache-max-entries", "50")
                .put("delta.table-statistics-cache-ttl", "10m")
                .build();

        DeltaConfig expected = new DeltaConfig()
                .setMaxSplitsBatchSize(400)
                .setParquetDereferencePushdownEnabled(false)
                .setCaseSensitivePartitionsEnabled(false)
                .setTableStatisticsEnabled(true)
                .setTableStatisticsCacheMaxEntries(50)
                .setTableStatisticsCacheTtl(new Duration(10, TimeUnit.MINUTES));

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
