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
package com.facebook.presto.lance;

import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;

public class TestLanceConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(LanceConfig.class)
                .setImpl("dir")
                .setRootUrl("")
                .setSingleLevelNs(true)
                .setReadBatchSize(8192)
                .setMaxRowsPerFile(1_000_000)
                .setMaxRowsPerGroup(100_000)
                .setWriteBatchSize(10_000)
                .setIndexCacheSize(new DataSize(128, MEGABYTE))
                .setMetadataCacheSize(new DataSize(128, MEGABYTE))
                .setDatasetCacheMaxEntries(100)
                .setDatasetCacheTtl(new Duration(60, TimeUnit.MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("lance.impl", "rest")
                .put("lance.root-url", "/data/lance")
                .put("lance.single-level-ns", "false")
                .put("lance.read-batch-size", "4096")
                .put("lance.max-rows-per-file", "500000")
                .put("lance.max-rows-per-group", "50000")
                .put("lance.write-batch-size", "5000")
                .put("lance.index-cache-size", "256MB")
                .put("lance.metadata-cache-size", "512MB")
                .put("lance.dataset-cache-max-entries", "50")
                .put("lance.dataset-cache-ttl", "30m")
                .build();

        LanceConfig expected = new LanceConfig()
                .setImpl("rest")
                .setRootUrl("/data/lance")
                .setSingleLevelNs(false)
                .setReadBatchSize(4096)
                .setMaxRowsPerFile(500_000)
                .setMaxRowsPerGroup(50_000)
                .setWriteBatchSize(5_000)
                .setIndexCacheSize(new DataSize(256, MEGABYTE))
                .setMetadataCacheSize(new DataSize(512, MEGABYTE))
                .setDatasetCacheMaxEntries(50)
                .setDatasetCacheTtl(new Duration(30, TimeUnit.MINUTES));

        assertFullMapping(properties, expected);
    }
}
