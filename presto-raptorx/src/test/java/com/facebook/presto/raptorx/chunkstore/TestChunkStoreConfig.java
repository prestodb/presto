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

public class TestChunkStoreConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ChunkStoreConfig.class)
                .setProvider(null)
                .setTimeoutThreads(1000)
                .setTimeout(new Duration(1, MINUTES))
                .setWriteThreads(5));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("chunk-store.provider", "file")
                .put("chunk-store.timeout", "42s")
                .put("chunk-store.max-timeout-threads", "13")
                .put("chunk-store.max-threads", "3")
                .build();

        ChunkStoreConfig expected = new ChunkStoreConfig()
                .setProvider("file")
                .setTimeout(new Duration(42, SECONDS))
                .setTimeoutThreads(13)
                .setWriteThreads(3);

        assertFullMapping(properties, expected);
    }
}
