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
