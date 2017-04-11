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
package com.facebook.presto.memory;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestMemoryManagerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(MemoryManagerConfig.class)
                .setKillOnOutOfMemory(false)
                .setKillOnOutOfMemoryDelay(new Duration(5, MINUTES))
                .setMaxQueryMemory(new DataSize(20, GIGABYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("query.low-memory-killer.enabled", "true")
                .put("query.low-memory-killer.delay", "20s")
                .put("query.max-memory", "2GB")
                .build();

        MemoryManagerConfig expected = new MemoryManagerConfig()
                .setKillOnOutOfMemory(true)
                .setKillOnOutOfMemoryDelay(new Duration(20, SECONDS))
                .setMaxQueryMemory(new DataSize(2, GIGABYTE));

        assertFullMapping(properties, expected);
    }
}
