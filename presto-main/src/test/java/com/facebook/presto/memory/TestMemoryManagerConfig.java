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
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.units.DataSize.Unit.GIGABYTE;

public class TestMemoryManagerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(MemoryManagerConfig.class)
                .setMaxQueryMemory(new DataSize(20, GIGABYTE))
                .setMaxQueryMemoryPerNode(new DataSize(1, GIGABYTE))
                .setClusterMemoryManagerEnabled(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("query.max-memory", "2GB")
                .put("query.max-memory-per-node", "2GB")
                .put("experimental.cluster-memory-manager-enabled", "false")
                .build();

        MemoryManagerConfig expected = new MemoryManagerConfig()
                .setMaxQueryMemory(new DataSize(2, GIGABYTE))
                .setMaxQueryMemoryPerNode(new DataSize(2, GIGABYTE))
                .setClusterMemoryManagerEnabled(false);

        assertFullMapping(properties, expected);
    }
}
