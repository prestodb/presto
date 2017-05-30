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
package com.facebook.presto.raptorx.chunkstore;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestChunkStoreCleanerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ChunkStoreCleanerConfig.class)
                .setInterval(new Duration(5, MINUTES))
                .setThreads(50));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("chunk-store.cleaner.interval", "33s")
                .put("chunk-store.cleaner.max-threads", "42")
                .build();

        ChunkStoreCleanerConfig expected = new ChunkStoreCleanerConfig()
                .setInterval(new Duration(33, SECONDS))
                .setThreads(42);

        assertFullMapping(properties, expected);
    }
}
